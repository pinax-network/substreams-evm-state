//! Module params: an account filter.
//!
//! Format: comma / whitespace separated list of 20-byte addresses, `0x`
//! prefix optional, case-insensitive. Empty string = no filter (all accounts).

use std::collections::HashSet;

#[derive(Debug, Default, Clone)]
pub struct Filter {
    accounts: HashSet<[u8; 20]>,
}

impl Filter {
    pub fn parse(params: &str) -> Result<Self, String> {
        let mut accounts = HashSet::new();
        for raw in params.split(|c: char| c == ',' || c.is_whitespace()) {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            let hexstr = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X")).unwrap_or(raw);
            let bytes = hex::decode(hexstr).map_err(|e| format!("invalid address '{raw}': {e}"))?;
            let addr: [u8; 20] = bytes
                .try_into()
                .map_err(|_| format!("invalid address '{raw}': expected 20 bytes"))?;
            accounts.insert(addr);
        }
        Ok(Self { accounts })
    }

    /// True when no filter is configured (every account passes).
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }

    pub fn len(&self) -> usize {
        self.accounts.len()
    }

    pub fn canonical(&self) -> String {
        let mut accounts: Vec<_> = self.accounts.iter().map(|a| format!("0x{}", hex::encode(a))).collect();
        accounts.sort();
        accounts.join(",")
    }

    pub fn matches(&self, address: &[u8]) -> bool {
        if self.accounts.is_empty() {
            return true;
        }
        match <[u8; 20]>::try_from(address) {
            Ok(a) => self.accounts.contains(&a),
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_matches_everything() {
        let f = Filter::parse("").unwrap();
        assert!(f.is_empty());
        assert!(f.matches(&[0u8; 20]));
    }

    #[test]
    fn parses_mixed_case_and_separators() {
        let f = Filter::parse(" 0xBB4CDB9CBD36B01BD1CBAEBF2DE08D9173BC095C,\n0000f90827f1c53a10cb7a02335b175320002935 ").unwrap();
        assert_eq!(f.len(), 2);
        assert!(f.matches(&hex::decode("bb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c").unwrap()));
        assert!(f.matches(&hex::decode("0000f90827f1c53a10cb7a02335b175320002935").unwrap()));
        assert!(!f.matches(&[1u8; 20]));
    }

    #[test]
    fn rejects_bad_length() {
        assert!(Filter::parse("0x1234").is_err());
        assert!(Filter::parse("0xzz").is_err());
    }
}
