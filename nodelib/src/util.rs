use rand::prelude::SliceRandom;

/// Characters for encoding to base 36.
pub static BASE36_DIGITS: &[char] = &[
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
    'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

/// Generate a random alphanumeric string.
pub fn random_string(len: usize) -> String {
    let mut rng = rand::thread_rng();
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        out.push(*BASE36_DIGITS.choose(&mut rng).unwrap());
    }
    out
}
