use reqwest::blocking::Client;
use reqwest::Url;
use std::process;
use std::str::FromStr;

use nodelib::dns::Namespace;
use nodelib::util::is_valid_dns_label;

use crate::util::*;

#[derive(Debug, clap::Args)]
pub struct ListArgs {
    /// Domain name
    from: String,
}

#[derive(Debug, clap::Args)]
pub struct IdentifyArgs {
    /// Domain name.
    from: String,

    /// Domain name.
    to: String,
}

/// List all aliases.
pub fn cmd_aliases(apid_url: &Url, args: ListArgs) {
    let (from_ns, from_hn) = parse_domainname(&args.from);
    let list_aliases_url = apid_url
        .join(&format!("/dns_aliases/{from_ns}/{from_hn}"))
        .unwrap();
    let response = reqwest_error(Client::new().get(list_aliases_url).send());
    if response.status().is_success() {
        let tos: Vec<String> = response.json().unwrap();
        println!("from\tto");
        for to in tos {
            println!("{from}\t{to}", from = args.from);
        }
    } else {
        print_error(response);
        process::exit(1);
    }
}

/// Create an alias.  This is idempotent, so it succeeds even if the alias
/// already exists.
pub fn cmd_create_alias(apid_url: &Url, args: IdentifyArgs) {
    let create_alias_url = alias_url(apid_url, &args.from, &args.to);
    let response = reqwest_error(Client::new().put(create_alias_url).send());
    if response.status().is_success() {
        println!("created");
    } else {
        print_error(response);
        process::exit(1);
    }
}

/// Delete an alias.
pub fn cmd_delete_alias(apid_url: &Url, args: IdentifyArgs) {
    let delete_alias_url = alias_url(apid_url, &args.from, &args.to);
    let response = reqwest_error(Client::new().delete(delete_alias_url).send());
    if response.status().is_success() {
        println!("deleted");
    } else {
        print_error(response);
        process::exit(1);
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Parse a domain name into a namespace and hostname, or die.
fn parse_domainname(s: &str) -> (Namespace, String) {
    if let Some((prefix, _)) = s.rsplit_once(".cluster.local") {
        if let Some((hn, ns)) = prefix.split_once('.') {
            if !is_valid_dns_label(hn) {
                eprintln!("could not parse domain '{s}': '{hn}' is not a valid DNS label");
                process::exit(1);
            }
            if let Ok(namespace) = Namespace::from_str(ns) {
                (namespace, hn.to_string())
            } else {
                eprintln!("could not parse domain '{s}': '{ns}' is not a valid namespace");
                process::exit(1);
            }
        } else {
            eprintln!("could not parse domain '{s}': should be in the form 'HOST.NAMESPACE.cluster.local'");
            process::exit(1);
        }
    } else {
        eprintln!(
            "could not parse domain '{s}': should be in the form 'HOST.NAMESPACE.cluster.local'"
        );
        process::exit(1);
    }
}

/// URL to get, put, or delete an alias
fn alias_url(apid_url: &Url, from: &str, to: &str) -> Url {
    let (from_ns, from_hn) = parse_domainname(from);
    let (to_ns, to_hn) = parse_domainname(to);
    apid_url
        .join(&format!("/dns_aliases/{from_ns}/{from_hn}/{to_ns}/{to_hn}"))
        .unwrap()
}
