/// Check that a name is all lowercase and is a valid DNS label.
pub fn is_valid_resource_name(name: &str) -> bool {
    let valid_character = |c: char| c.is_ascii_alphanumeric() || c == '-';

    !name.is_empty()
        && name.len() <= 63
        && name.chars().all(valid_character)
        && name.starts_with(|c: char| c.is_ascii_alphabetic())
        && !name.ends_with(|c: char| c == '-')
}
