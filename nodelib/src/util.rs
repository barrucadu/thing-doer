use rand::prelude::SliceRandom;
use std::net::{IpAddr, SocketAddr};

/// Characters for encoding to base 36.
pub static BASE36_DIGITS: &[char] = &[
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
    'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

/// Check that a name is all lowercase and is a valid DNS label.
pub fn is_valid_resource_name(name: &str) -> bool {
    let valid_character = |c: char| c.is_ascii_alphanumeric() || c == '-';

    !name.is_empty()
        && name.len() <= 63
        && name.chars().all(valid_character)
        && name.starts_with(|c: char| c.is_ascii_alphabetic())
        && !name.ends_with(|c: char| c == '-')
}

/// Check that a type name is nonempty and of the form
/// `/[a-z0-9][a-z0-9\.]*[a-z0-9]`.
pub fn is_valid_resource_type(rtype: &str) -> bool {
    let valid_character = |c: char| c.is_ascii_alphanumeric() || c == '.';

    !rtype.is_empty()
        && rtype.chars().all(valid_character)
        && !rtype.starts_with(|c: char| c == '.')
        && !rtype.ends_with(|c: char| c == '.')
}

/// Convert a SocketAddr into a resource name / DNS label.
pub fn sockaddr_to_name(addr: SocketAddr) -> String {
    let ipv6 = match addr.ip() {
        IpAddr::V4(ip) => ip.to_ipv6_mapped(),
        IpAddr::V6(ip) => ip,
    };
    let address_str = encode_number(u128::from_be_bytes(ipv6.octets()), BASE36_DIGITS);
    let port_str = encode_number(addr.port().into(), BASE36_DIGITS);

    let mut rng = rand::thread_rng();
    let r1 = BASE36_DIGITS.choose(&mut rng).unwrap();
    let r2 = BASE36_DIGITS.choose(&mut rng).unwrap();
    let r3 = BASE36_DIGITS.choose(&mut rng).unwrap();
    let r4 = BASE36_DIGITS.choose(&mut rng).unwrap();

    format!("a{address_str}-p{port_str}-{r1}{r2}{r3}{r4}")
}

/// Encode a number with a given digit sequence.
pub fn encode_number(mut num: u128, digits: &[char]) -> String {
    if num == 0 {
        return digits[0].to_string();
    }

    let b = digits.len() as u128;

    let mut out = String::with_capacity(25);
    while num > 0 {
        let q = num / b;
        let r = num % b;
        out.insert(0, digits[r as usize]);
        num = q;
    }
    out
}
