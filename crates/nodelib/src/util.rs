use rand::prelude::SliceRandom;

/// Ominous adjectives
pub static FIRST_PART: &[&str] = &[
    "abnormal",
    "accursed",
    "amorphous",
    "antediluvian",
    "antique",
    "atavistic",
    "blasphemous",
    "charnel",
    "cthonian",
    "cyclopean",
    "dank",
    "decadent",
    "demonic",
    "eldritch",
    "fungoid",
    "furtive",
    "gibbering",
    "gibbous",
    "hideous",
    "hoary",
    "indescribable",
    "loath",
    "mortal",
    "nameless",
    "noisome",
    "noneuclidean",
    "shunned",
    "spectral",
    "squamous",
    "stygian",
    "unmentionable",
    "unutterable",
];

/// Cute nouns
pub static LAST_PART: &[&str] = &[
    "axolotl",
    "bat",
    "bear",
    "bumblebee",
    "capybara",
    "cat",
    "fox",
    "gecko",
    "goat",
    "hedgehog",
    "kitten",
    "koala",
    "lemming",
    "loris",
    "mink",
    "mole",
    "otter",
    "owl",
    "panda",
    "penguin",
    "pika",
    "puppy",
    "quokka",
    "rabbit",
    "seal",
    "sheep",
    "shrew",
    "snail",
    "squirrel",
    "tanuki",
    "weasel",
    "wombat",
];

/// Digits
pub static DIGITS: &[char] = &['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];

/// Generate a random name, suitable for a resource.
pub fn random_name() -> String {
    let mut rng = rand::thread_rng();

    format!(
        "{first_part}-{last_part}-{d1}{d2}{d3}{d4}",
        first_part = FIRST_PART.choose(&mut rng).unwrap(),
        last_part = LAST_PART.choose(&mut rng).unwrap(),
        d1 = DIGITS.choose(&mut rng).unwrap(),
        d2 = DIGITS.choose(&mut rng).unwrap(),
        d3 = DIGITS.choose(&mut rng).unwrap(),
        d4 = DIGITS.choose(&mut rng).unwrap(),
    )
}

/// Check that a string is all lowercase and is a valid DNS label.
pub fn is_valid_dns_label(s: &str) -> bool {
    let valid_character = |c: char| c.is_ascii_alphanumeric() || c == '-';

    !s.is_empty()
        && s.len() <= 63
        && s.chars().all(valid_character)
        && s.starts_with(|c: char| c.is_ascii_alphabetic())
        && !s.ends_with(|c: char| c == '-')
}
