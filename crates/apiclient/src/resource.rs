use reqwest::blocking::Client;
use reqwest::Url;
use serde::Serialize;
use serde_json::Value;
use std::fs::File;
use std::io::BufReader;
use std::process;

use nodelib::resources::Resource;

use crate::util::*;

#[derive(Debug, clap::Args)]
pub struct ApplyArgs {
    /// File containing the resource json.  `-` means read from stdin.
    #[arg(default_value = "-")]
    file: String,
}

#[derive(Debug, clap::Args)]
pub struct ListArgs {
    /// Resource type.
    rtype: String,
}

#[derive(Debug, clap::Args)]
pub struct IdentifyArgs {
    /// Resource type.
    rtype: String,

    /// Resource name.
    rname: String,
}

/// Create or patch a resource.
pub fn cmd_apply(apid_url: &Url, args: ApplyArgs) {
    let json = read_json_from_file_or_stdin(&args.file);
    if let (Some(rname), Some(rtype)) = (json["name"].as_str(), json["type"].as_str()) {
        let patch_resources_url = resource_url(apid_url, rtype, rname);

        let mut patch = json.clone();
        if let Some(obj) = patch.as_object_mut() {
            obj.remove("name");
            obj.remove("type");
        }

        let response = reqwest_error(Client::new().patch(patch_resources_url).json(&patch).send());
        if response.status().is_success() {
            println!("{body}", body = response.text().unwrap());
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            match Resource::try_from(json.clone()) {
                Ok(resource) => {
                    let body = create_or_die_on_error(apid_url, &resource);
                    println!("{body}");
                }
                Err(error) => {
                    eprintln!("error: {error:?}");
                    process::exit(1);
                }
            }
        } else {
            print_error(response);
            process::exit(1);
        }
    } else {
        eprintln!("error: expected 'name' and 'type'");
        process::exit(1);
    }
}

/// List all matching resources.
pub fn cmd_list(apid_url: &Url, args: ListArgs) {
    let list_resources_url = apid_url
        .join(&format!("/resources/{rtype}", rtype = args.rtype))
        .unwrap();
    let response = reqwest_error(Client::new().get(list_resources_url).send());
    if response.status().is_success() {
        let resources: Vec<Resource> = response.json().unwrap();
        println!("name\ttype\tstate");
        for r in resources {
            println!(
                "{rname}\t{rtype}\t{state}",
                rname = r.name,
                rtype = r.rtype,
                state = r.state.unwrap_or_default()
            );
        }
    } else {
        print_error(response);
        process::exit(1);
    }
}

/// Get a single resource.
pub fn cmd_get(apid_url: &Url, args: IdentifyArgs) {
    let get_resource_url = resource_url(apid_url, &args.rtype, &args.rname);
    let response = reqwest_error(Client::new().get(get_resource_url).send());
    if response.status().is_success() {
        println!("{body}", body = response.text().unwrap());
    } else {
        print_error(response);
        process::exit(1);
    }
}

/// Delete a single resource.  If this is a running pod, it is killed.
pub fn cmd_delete(apid_url: &Url, args: IdentifyArgs) {
    let delete_resource_url = resource_url(apid_url, &args.rtype, &args.rname);
    let response = reqwest_error(Client::new().delete(delete_resource_url).send());
    if response.status().is_success() {
        println!("deleted");
    } else {
        print_error(response);
        process::exit(1);
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Create a resource, or die on error.
pub fn create_or_die_on_error<T: Serialize>(apid_url: &Url, resource: &T) -> String {
    let post_resources_url = apid_url.join("resources").unwrap();
    let response = reqwest_error(Client::new().post(post_resources_url).json(resource).send());
    if response.status().is_success() {
        response.text().unwrap()
    } else {
        print_error(response);
        process::exit(1);
    }
}

/// URL to get, patch, or delete a single resource
fn resource_url(apid_url: &Url, rtype: &str, rname: &str) -> Url {
    apid_url
        .join(&format!("/resources/{rtype}/{rname}"))
        .unwrap()
}

///////////////////////////////////////////////////////////////////////////////

fn read_json_from_file_or_stdin(path: &str) -> Value {
    let json = if path == "-" {
        serde_json::from_reader(std::io::stdin().lock())
    } else {
        match File::open(path) {
            Ok(file) => serde_json::from_reader(BufReader::new(file)),
            Err(error) => {
                eprintln!("could not read '{path}': {error}");
                process::exit(1);
            }
        }
    };

    match json {
        Ok(value) => value,
        Err(error) => {
            eprintln!("could not read '{path}': {error}");
            process::exit(1);
        }
    }
}
