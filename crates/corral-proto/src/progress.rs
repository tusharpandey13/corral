//! Progress lines: `##corral progress=<0-100> <text>` (R-IO-06).

/// Parse one output line. Malformed lines return `None` and are passed through as plain output.
pub fn parse_progress(line: &str) -> Option<(u8, String)> {
    let rest = line.trim_end_matches(['\r', '\n']).strip_prefix("##corral progress=")?;
    let (num, text) = match rest.split_once(' ') {
        Some((n, t)) => (n, t.trim()),
        None => (rest, ""),
    };
    let pct: u8 = num.parse().ok().filter(|p| *p <= 100)?;
    Some((pct, text.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// U-19
    #[test]
    fn parses_and_rejects() {
        assert_eq!(parse_progress("##corral progress=42 building"), Some((42, "building".into())));
        assert_eq!(parse_progress("##corral progress=100\n"), Some((100, String::new())));
        assert_eq!(parse_progress("##corral progress=101 x"), None);
        assert_eq!(parse_progress("##corral progress=abc x"), None);
        assert_eq!(parse_progress("  ##corral progress=5 x"), None);
        assert_eq!(parse_progress("hello"), None);
    }
}
