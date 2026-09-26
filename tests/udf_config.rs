//! Guards on the shipped UDF registration file.
//!
//! Both failure modes below have bitten this project: ClickHouse rejects the
//! whole file for a malformed comment, and a precision mismatch between the
//! command line and the declared return type produces a column of zeros
//! instead of an error.

const CONFIG: &str = include_str!("../clickhouse/zetasketch_clickhouse_function.xml");

#[test]
fn comments_contain_no_double_hyphen() {
    // XML forbids "--" inside a comment; ClickHouse's SAX parser rejects the
    // entire file, which surfaces only as "function does not exist".
    let mut rest = CONFIG;
    while let Some(start) = rest.find("<!--") {
        let body_start = start + 4;
        let end = rest[body_start..]
            .find("-->")
            .expect("unterminated XML comment");
        let body = &rest[body_start..body_start + end];
        assert!(
            !body.contains("--"),
            "XML comment contains a double hyphen, which makes the file unparseable:\n{body}"
        );
        rest = &rest[body_start + end + 3..];
    }
}

#[test]
fn the_root_element_is_functions_not_clickhouse() {
    // Standalone *_function.xml files are parsed on their own, so wrapping
    // them in <clickhouse> hides the definition from the loader.
    let markup = strip_comments(CONFIG);
    let first_tag = markup
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with('<'))
        .expect("no elements in config");
    assert_eq!(first_tag, "<functions>", "root element must be <functions>");
}

#[test]
fn the_command_precision_matches_the_declared_return_type() {
    let markup = strip_comments(CONFIG);
    let return_type = between(&markup, "<return_type>", "</return_type>");
    let command = between(&markup, "<command>", "</command>");

    let declared = between(&return_type, "uniqCombined64(", ")");
    let passed = command
        .split_whitespace()
        .skip_while(|token| *token != "--precision")
        .nth(1)
        .expect("command has no precision flag");

    assert_eq!(
        declared, passed,
        "return type says uniqCombined64({declared}) but the command passes {passed}; \
         a mismatch silently yields empty states"
    );
}

#[test]
fn the_command_invokes_the_binary_by_bare_name() {
    // <command> resolves against user_scripts_path, so an absolute path fails
    // with "does not exist inside user scripts folder".
    let command = between(&strip_comments(CONFIG), "<command>", "</command>");
    let program = command.split_whitespace().next().unwrap();
    assert!(
        !program.starts_with('/'),
        "command must be relative to user_scripts_path, got {program}"
    );
    assert_eq!(program, "zetasketch-clickhouse");
}

#[test]
fn chunk_headers_are_requested() {
    // The UDF reads a row count before each chunk; without this the framing
    // the binary expects would not match what ClickHouse sends.
    let markup = strip_comments(CONFIG);
    assert_eq!(between(&markup, "<send_chunk_header>", "</send_chunk_header>"), "true");
    assert_eq!(between(&markup, "<format>", "</format>"), "RowBinary");
}

/// Drops comment bodies so the assertions read the actual markup and not the
/// tag names discussed in the comments.
fn strip_comments(xml: &str) -> String {
    let mut out = String::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<!--") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 4..];
        let end = after.find("-->").expect("unterminated XML comment");
        rest = &after[end + 3..];
    }
    out.push_str(rest);
    out
}

fn between(haystack: &str, open: &str, close: &str) -> String {
    let start = haystack.find(open).unwrap_or_else(|| panic!("missing {open}")) + open.len();
    let end = start
        + haystack[start..]
            .find(close)
            .unwrap_or_else(|| panic!("missing {close}"));
    haystack[start..end].trim().to_string()
}
