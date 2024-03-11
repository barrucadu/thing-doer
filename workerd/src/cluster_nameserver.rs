use bytes::BytesMut;
use std::collections::HashMap;
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
        empty_zone,
        // will be filled in by the `Watcher` impl
        zones: Zones::default(),
    }));
    watcher::setup_watcher(
        &etcd_config,
        state.clone(),
        prefix::domain_name(&etcd_config),
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
    pub empty_zone: Zone,
    pub zones: Zones,
}

impl watcher::Watcher for WatchState {
    async fn apply_event(&mut self, event: Event) {
        let prefix = prefix::domain_name(&self.etcd_config);
        let is_create = event.r#type() == EventType::Put;
        let kv = event.kv.unwrap();
        let key = String::from_utf8(kv.key).unwrap();

        if let Some((_, name)) = key.split_once(&prefix) {
            if let Some(domain) = DomainName::from_dotted_string(name) {
                if is_create {
                    match String::from_utf8(kv.value) {
                        Ok(address_str) => match Ipv4Addr::from_str(&address_str) {
                            Ok(address) => {
                                tracing::info!(name, ?address, "got new A record");
                                self.a_records.insert(domain, address);
                            }
                            Err(error) => tracing::warn!(name, ?error, "could not parse A record"),
                        },
                        Err(error) => tracing::warn!(name, ?error, "could not parse A record"),
                    }
                } else {
                    self.a_records.remove(&domain);
                }
            } else {
                tracing::warn!(?key, "could not parse domain name");
            }
        } else {
            tracing::warn!(?key, "unexpected watch key");
        }

        let mut zone = self.empty_zone.clone();
        for (name, address) in &self.a_records {
            // use a TTL of 0 so they just inherit the value from the zone
            zone.insert(name, RecordTypeWithData::A { address: *address }, 0)
        }

        let mut zones = Zones::default();
        zones.insert(zone);

        self.zones = zones;
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
    let (tx, mut rx) = mpsc::channel(128);
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
                        if let Err(error) = tx_clone.send((message, peer)).await {
                            tracing::debug!(?peer, ?error, "could not send UDP DNS response");
                        }
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
        )
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
