use sha2::{Digest, Sha256};

/// Lowercase hex SHA-256, as used for `output_id` and `fixture_sha256`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_contract_example() {
        assert_eq!(
            sha256_hex(b"battery-health|TST00000000000001|1790000100000"),
            "924bbe50976ec7a530dd47f70c1c42f19bb46342e4e320a06a3e076bb70feda8"
        );
    }
}
