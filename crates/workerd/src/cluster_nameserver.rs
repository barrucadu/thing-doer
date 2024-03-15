use bytes::BytesMut;
use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::process;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio::sync::RwLock;

use dns_resolver::cache::SharedCache;
use dns_resolver::resolve;
use dns_resolver::util::net::*;
use dns_resolver::util::types::{ProtocolMode, ResolvedRecord};
use dns_types::protocol::types::*;
use dns_types::zones::types::*;

use nodelib::dns::cluster_zone;
use nodelib::error::*;
use nodelib::etcd;
use nodelib::etcd::pb::mvccpb::{event::EventType, Event};
use nodelib::etcd::prefix;
use nodelib::etcd::watcher;

/// How frequently to expire / prune the cache.
pub static PRUNE_CACHE_INTERVAL: u64 = 300;

/// Start a forwarding DNS resolver.
pub async fn initialise(
    etcd_config: etcd::Config,
    my_name: &str,
    bind_address: Ipv4Addr,
    forward_address: SocketAddr,
) -> Result<(), Error> {
    tracing::info!(address=?bind_address, port=?53, "binding DNS UDP socket");
    let udp = match UdpSocket::bind((bind_address, 53)).await {
        Ok(s) => s,
        Err(error) => {
            tracing::error!(?error, "could not bind DNS UDP socket");
            // TODO: return an error
            process::exit(1);
        }
    };

    tracing::info!(address=?bind_address, port=?53, "binding DNS TCP socket");
    let tcp = match TcpListener::bind((bind_address, 53)).await {
        Ok(s) => s,
        Err(error) => {
            tracing::error!(?error, "could not bind DNS TCP socket");
            // TODO: return an error
            process::exit(1);
        }
    };

    let empty_zone = cluster_zone(my_name);
    let state = Arc::new(RwLock::new(WatchState {
        etcd_config: etcd_config.clone(),
        a_records: HashMap::new(),
        alias_records: HashMap::new(),
        empty_zone,
        // will be filled in by the `Watcher` impl
        zones: Zones::default(),
    }));
    watcher::setup_watcher(
        &etcd_config,
        state.clone(),
        vec![prefix::domain_name(&etcd_config)],
    )
    .await?;

    let cache = SharedCache::with_desired_size(4096);
    tokio::spawn(listen_task(
        forward_address,
        cache.clone(),
        state.clone(),
        udp,
        tcp,
    ));
    tokio::spawn(prune_cache_task(cache));

    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

/// Cluster DNS records
#[derive(Debug)]
struct WatchState {
    pub etcd_config: etcd::Config,
    pub a_records: HashMap<DomainName, Ipv4Addr>,
    pub alias_records: HashMap<DomainName, HashSet<DomainName>>,
    pub empty_zone: Zone,
    pub zones: Zones,
}

impl watcher::Watcher for WatchState {
    async fn apply_event(&mut self, event: Event) {
        let prefix = prefix::domain_name(&self.etcd_config);
        let is_create = event.r#type() == EventType::Put;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, suffix)) = key.split_once(&prefix) {
            if let Some((from, to)) = suffix.split_once('/') {
                self.add_or_remove_alias_record(is_create, from, to);
            } else {
                self.add_or_remove_a_record(is_create, suffix, kv.value);
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }

        self.regenerate_zones();
    }
}

impl WatchState {
    fn add_or_remove_a_record(&mut self, is_create: bool, name: &str, value: Vec<u8>) {
        if let Some(named) = DomainName::from_dotted_string(name) {
            if is_create {
                match String::from_utf8(value) {
                    Ok(address_str) => match Ipv4Addr::from_str(&address_str) {
                        Ok(address) => {
                            tracing::info!(name, ?address, "got new A record");
                            self.a_records.insert(named, address);
                        }
                        Err(error) => tracing::warn!(name, ?error, "could not parse A record"),
                    },
                    Err(error) => tracing::warn!(name, ?error, "could not parse A record"),
                }
            } else {
                tracing::info!(name, "removed A record");
                self.a_records.remove(&named);
            }
        } else {
            tracing::warn!(name, "could not parse domain name");
        }
    }

    fn add_or_remove_alias_record(&mut self, is_create: bool, from: &str, to: &str) {
        match (
            DomainName::from_dotted_string(from),
            DomainName::from_dotted_string(to),
        ) {
            (Some(fromd), Some(tod)) => {
                let mut aliases = self.alias_records.remove(&fromd).unwrap_or_default();
                if is_create {
                    tracing::info!(from, to, "got new alias record");
                    aliases.insert(tod);
                } else {
                    tracing::info!(from, to, "removed alias record");
                    aliases.remove(&tod);
                }
                if !aliases.is_empty() {
                    self.alias_records.insert(fromd, aliases);
                }
            }
            _ => {
                tracing::warn!(from, to, "could not parse domain name");
            }
        }
    }

    // TODO: make this a bit more efficient than recomputing everything on every
    // change
    fn regenerate_zones(&mut self) {
        let mut records = HashMap::with_capacity(self.a_records.len() + self.alias_records.len());

        for (name, address) in &self.a_records {
            records.insert(name.clone(), HashSet::from([*address]));
        }
        for name in self.alias_records.keys() {
            self.resolve_alias_records(&mut records, name);
        }

        let mut zone = self.empty_zone.clone();
        for (name, addresses) in records.drain() {
            for address in addresses {
                // use a TTL of 0 so they just inherit the value from the zone
                zone.insert(&name, RecordTypeWithData::A { address }, 0);
            }
        }
        let mut zones = Zones::default();
        zones.insert(zone);

        self.zones = zones;
    }

    fn resolve_alias_records(
        &self,
        records: &mut HashMap<DomainName, HashSet<Ipv4Addr>>,
        name: &DomainName,
    ) -> HashSet<Ipv4Addr> {
        if let Some(rs) = records.get(name) {
            return rs.clone();
        }

        if let Some(aliases) = self.alias_records.get(name) {
            let mut rs = HashSet::with_capacity(aliases.len());
            for alias in aliases {
                rs = rs
                    .union(&self.resolve_alias_records(records, alias))
                    .copied()
                    .collect::<HashSet<Ipv4Addr>>();
            }
            records.insert(name.clone(), rs.clone());
            rs
        } else {
            HashSet::new()
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Listen to and answer incoming DNS queries.
async fn listen_task(
    forward_address: SocketAddr,
    cache: SharedCache,
    state: Arc<RwLock<WatchState>>,
    udp: UdpSocket,
    tcp: TcpListener,
) {
    // The write end of a UdpSocket can't be cloned, so this channel hack is
    // needed.
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut buf = vec![0u8; 512];

    loop {
        tokio::select! {
            Ok((size, peer)) = udp.recv_from(&mut buf) => {
                let bytes = BytesMut::from(&buf[..size]);
                let cache_clone = cache.clone();
                let state_clone = state.clone();
                let tx_clone = tx.clone();
                tokio::spawn(async move {
                    if let Some(message) = handle_raw_message(forward_address, cache_clone, state_clone, bytes.as_ref()).await {
                        tx_clone.send((message, peer)).expect("could not send to unbounded channel");
                    }
                });
            }

            Some((message, peer)) = rx.recv() => {
                let mut serialised = message.to_octets().unwrap();
                if let Err(error) = send_udp_bytes_to(&udp, peer, &mut serialised).await {
                    tracing::debug!(?peer, ?error, "could not send UDP DNS response");
                }
            }

            Ok((mut stream, peer)) = tcp.accept() => {
                let cache_clone = cache.clone();
                let state_clone = state.clone();
                tokio::spawn(async move {
                    if let Ok(bytes) = read_tcp_bytes(&mut stream).await {
                        if let Some(message) = handle_raw_message(forward_address, cache_clone, state_clone, bytes.as_ref()).await {
                            let mut serialised = message.to_octets().unwrap();
                            if let Err(error) = send_tcp_bytes(&mut stream, &mut serialised).await {
                                tracing::debug!(?peer, ?error, "could not send TCP DNS response");
                            }
                        }
                    }
                });
            }
        }
    }
}

/// Delete expired cache entries every `PRUNE_CACHE_INTERVAL` seconds.
///
/// Always removes all expired entries, and then if the cache is still too big
/// prunes it down to size.
async fn prune_cache_task(cache: SharedCache) {
    loop {
        tokio::time::sleep(Duration::from_secs(PRUNE_CACHE_INTERVAL)).await;

        let (overflow, current_size, expired, pruned) = cache.prune();
        tracing::info!(
            ?overflow,
            ?current_size,
            ?expired,
            ?pruned,
            "pruned DNS cache"
        );
    }
}

///////////////////////////////////////////////////////////////////////////////

async fn handle_raw_message(
    forward_address: SocketAddr,
    cache: SharedCache,
    state: Arc<RwLock<WatchState>>,
    buf: &[u8],
) -> Option<Message> {
    match Message::from_octets(buf) {
        Ok(msg) => {
            if msg.header.is_response {
                // Do not respond to response messages: this is because an
                // inbound message could spoof its source address / port to
                // match the server's, and so make it respond to itself, which
                // triggers another response, etc
                None
            } else if msg.header.opcode == Opcode::Standard {
                Some(resolve_and_build_response(forward_address, cache, state, msg).await)
            } else {
                let mut response = msg.make_response();
                response.header.rcode = Rcode::NotImplemented;
                Some(response)
            }
        }

        // An attacker could craft an incomplete message with the source address
        // / port being resolved's, which would make resolved respond to itself
        // here, but this is fine so long as (1) the response we send is valid
        // and (2) we don't reply to a valid message which is a response.
        Err(err) => err.id().map(Message::make_format_error_response),
    }
}

/// Construct a response to a DNS query.
async fn resolve_and_build_response(
    forward_address: SocketAddr,
    cache: SharedCache,
    state: Arc<RwLock<WatchState>>,
    query: Message,
) -> Message {
    let mut response = query.make_response();
    response.header.recursion_available = true;

    match triage(&query) {
        Err(rcode) => {
            response.header.rcode = rcode;
        }
        Ok(None) => {}
        Ok(Some(question)) => {
            let r = state.read().await;

            let (_, answer) = resolve(
                query.header.recursion_desired,
                ProtocolMode::PreferV4,
                53,
                Some(forward_address),
                &r.zones,
                &cache,
                question,
            )
            .await;

            match answer {
                Ok(ResolvedRecord::Authoritative { mut rrs, soa_rr }) => {
                    response.answers.append(&mut rrs);
                    response.authority.push(soa_rr);
                    response.header.is_authoritative = true;
                }
                Ok(ResolvedRecord::AuthoritativeNameError { soa_rr }) => {
                    response.authority.push(soa_rr);
                    response.header.rcode = Rcode::NameError;
                    response.header.is_authoritative = true;
                }
                Ok(ResolvedRecord::NonAuthoritative { mut rrs, soa_rr }) => {
                    response.answers.append(&mut rrs);
                    if let Some(soa_rr) = soa_rr {
                        response.authority.push(soa_rr);
                    }
                    response.header.is_authoritative = false;
                }
                Err(_) => (),
            }
        }
    }

    if response.answers.is_empty()
        && response.authority.is_empty()
        && response.header.rcode == Rcode::NoError
    {
        response.header.rcode = Rcode::ServerFailure;
        response.header.is_authoritative = false;
    }

    response
}

/// Decide if a question can even be answered
fn triage(query: &Message) -> Result<Option<&Question>, Rcode> {
    if query.questions.is_empty() {
        Ok(None)
    } else if query.questions.len() == 1 {
        let question = &query.questions[0];
        if question.is_unknown() {
            Err(Rcode::Refused)
        } else {
            Ok(Some(question))
        }
    } else {
        Err(Rcode::Refused)
    }
}
