use std::fmt;
use std::net::Ipv4Addr;
use std::str::FromStr;
use tonic::Request;

use dns_types::protocol::types::DomainName;
use dns_types::zones::types::{Zone, SOA};

use crate::error::Error;
use crate::etcd;
use crate::etcd::leaser::LeaseId;
use crate::etcd::pb::etcdserverpb::{PutRequest, RangeRequest};
use crate::etcd::prefix;
use crate::resources::node::NodeType;
use crate::util;

/// Apex for the cluster DNS zone.
pub static APEX: &str = "cluster.local.";

/// Mailbox of the DNS administrator - I think this is basically irrelevant, but
/// it has to be specified, so just use `admin.cluster.local`.
pub static SOA_RNAME: &str = "admin.cluster.local.";

/// Version number of the zone - I think the choice here is either leaving it
/// static forever, or incrementing it every time a record is added or removed.
/// Leaving it static seems fine for now.
pub static SOA_SERIAL: u32 = 1;

/// If some other nameserver caches this zone, how often it should refresh its
/// cached copy.
pub static SOA_REFRESH: u32 = 300;

/// If the refresh fails, how long to wait before trying again.
pub static SOA_RETRY: u32 = 30;

/// If some other nameserver caches this zone, the maximum amount of time that
/// the cached copy is valid.
pub static SOA_EXPIRE: u32 = 300;

/// The minimum TTL for records in this zone.  This is also used as the TTL for
/// our cluster DNS records.
pub static SOA_MINIMUM: u32 = 5;

/// Construct the zone that a worker should serve.  The worker's domain name is
/// used as the SOA MNAME.
pub fn cluster_zone(worker_node_name: &str) -> Zone {
    Zone::new(
        DomainName::from_dotted_string(APEX).unwrap(),
        Some(SOA {
            mname: DomainName::from_dotted_string(&domain_name_for(
                &Namespace::Node(NodeType::Worker),
                worker_node_name,
            ))
            .unwrap(),
            rname: DomainName::from_dotted_string(SOA_RNAME).unwrap(),
            serial: SOA_SERIAL,
            refresh: SOA_REFRESH,
            retry: SOA_RETRY,
            expire: SOA_EXPIRE,
            minimum: SOA_MINIMUM,
        }),
    )
}

///////////////////////////////////////////////////////////////////////////////

/// Create an `A` record bound to a lease.
pub async fn create_leased_a_record(
    etcd_config: &etcd::Config,
    lease_id: LeaseId,
    namespace: &Namespace,
    hostname: &str,
    address: Ipv4Addr,
) -> Result<(), Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    kv_client
        .put(Request::new(PutRequest {
            key: record_key(etcd_config, namespace, hostname).into(),
            value: address.to_string().into(),
            lease: lease_id.0,
            ..Default::default()
        }))
        .await?;

    Ok(())
}

/// Delete a record without waiting for lease expiry.
///
/// Returns `false` if the record does not exist.
pub async fn delete_record(
    etcd_config: &etcd::Config,
    namespace: &Namespace,
    hostname: &str,
) -> Result<bool, Error> {
    etcd::util::delete_if_exists(etcd_config, record_key(etcd_config, namespace, hostname)).await
}

///////////////////////////////////////////////////////////////////////////////

/// List all aliases that a name points to.
pub async fn list_aliases(
    etcd_config: &etcd::Config,
    from_namespace: &Namespace,
    from_hostname: &str,
) -> Result<Vec<String>, Error> {
    let key_prefix = format!(
        "{prefix}/",
        prefix = record_key(etcd_config, from_namespace, from_hostname),
    );
    let (kvs, _) = etcd::util::list_kvs(etcd_config, key_prefix.clone(), None).await?;

    let mut out = Vec::with_capacity(kvs.len());
    for kv in kvs {
        let key = String::from_utf8(kv.key).unwrap();
        let (_, name) = key.split_once(&key_prefix).unwrap();
        out.push(name.to_string());
    }

    Ok(out)
}

/// Check if an alias record exists.
pub async fn alias_record_exists(
    etcd_config: &etcd::Config,
    from_namespace: &Namespace,
    from_hostname: &str,
    to_namespace: &Namespace,
    to_hostname: &str,
) -> Result<bool, Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    let res = kv_client
        .range(Request::new(RangeRequest {
            key: alias_record_key(
                etcd_config,
                from_namespace,
                from_hostname,
                to_namespace,
                to_hostname,
            )
            .into(),
            ..Default::default()
        }))
        .await?
        .into_inner();

    Ok(!res.kvs.is_empty())
}

/// Add an alias record to a name, optionally associated with a lease.
pub async fn append_alias_record(
    etcd_config: &etcd::Config,
    lease_id: Option<LeaseId>,
    from_namespace: &Namespace,
    from_hostname: &str,
    to_namespace: &Namespace,
    to_hostname: &str,
) -> Result<(), Error> {
    let mut kv_client = etcd_config.kv_client().await?;
    kv_client
        .put(Request::new(PutRequest {
            key: alias_record_key(
                etcd_config,
                from_namespace,
                from_hostname,
                to_namespace,
                to_hostname,
            )
            .into(),
            value: b"alias".into(),
            lease: lease_id.map_or(0, |l| l.0),
            ..Default::default()
        }))
        .await?;

    Ok(())
}

/// Delete an alias record from a name without waiting for lease expiry.
///
/// Returns `false` if the record does not exist.
pub async fn delete_alias_record(
    etcd_config: &etcd::Config,
    from_namespace: &Namespace,
    from_hostname: &str,
    to_namespace: &Namespace,
    to_hostname: &str,
) -> Result<bool, Error> {
    etcd::util::delete_if_exists(
        etcd_config,
        alias_record_key(
            etcd_config,
            from_namespace,
            from_hostname,
            to_namespace,
            to_hostname,
        ),
    )
    .await
}

///////////////////////////////////////////////////////////////////////////////

/// Namespaces in which domains can live.  A domain in a namespace `foo` is a
/// subdomain of `foo.cluster.local.`.
#[derive(Debug, Clone)]
pub enum Namespace {
    Node(NodeType),
    Pod,
    Special,
    Custom(String),
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node(NodeType::Api) => write!(f, "api.node"),
            Self::Node(NodeType::Reaper) => write!(f, "reaper.node"),
            Self::Node(NodeType::Scheduler) => write!(f, "scheduler.node"),
            Self::Node(NodeType::Worker) => write!(f, "worker.node"),
            Self::Pod => write!(f, "pod"),
            Self::Special => write!(f, "special"),
            Self::Custom(s) => write!(f, "{s}"),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ParseNamespaceError;

impl FromStr for Namespace {
    type Err = ParseNamespaceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "api.node" => Ok(Self::Node(NodeType::Api)),
            "reaper.node" => Ok(Self::Node(NodeType::Reaper)),
            "scheduler.node" => Ok(Self::Node(NodeType::Scheduler)),
            "worker.node" => Ok(Self::Node(NodeType::Worker)),
            "pod" => Ok(Self::Pod),
            "special" => Ok(Self::Special),
            _ => {
                if util::is_valid_dns_label(s) && s != "node" {
                    Ok(Self::Custom(s.to_owned()))
                } else {
                    Err(ParseNamespaceError)
                }
            }
        }
    }
}

/// Construct a domain name.
pub fn domain_name_for(namespace: &Namespace, hostname: &str) -> String {
    format!("{hostname}.{namespace}.{APEX}")
}

///////////////////////////////////////////////////////////////////////////////

/// etcd key for a record
fn record_key(etcd_config: &etcd::Config, namespace: &Namespace, hostname: &str) -> String {
    format!(
        "{prefix}{domain}",
        prefix = prefix::domain_name(etcd_config),
        domain = domain_name_for(namespace, hostname),
    )
}

/// etcd key for an alias record - the `record_key` is a prefix of this
fn alias_record_key(
    etcd_config: &etcd::Config,
    from_namespace: &Namespace,
    from_hostname: &str,
    to_namespace: &Namespace,
    to_hostname: &str,
) -> String {
    format!(
        "{prefix}/{to_domain}",
        prefix = record_key(etcd_config, from_namespace, from_hostname),
        to_domain = domain_name_for(to_namespace, to_hostname),
    )
}
