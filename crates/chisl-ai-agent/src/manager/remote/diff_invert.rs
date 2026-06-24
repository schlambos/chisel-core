//! Unified-diff hunk parser, inverter, and applier.
//!
//! Provides fine-grained operations on individual hunks within a unified diff:
//! - Parse a patch into its constituent hunks
//! - Invert a single hunk (swap additions/deletions and ranges)
//! - Apply an inverted hunk to file content
//! - Revert a single hunk from a multi-hunk patch
//! - Invert an entire patch

use std::sync::LazyLock;

use chisl_common::AppError;
use regex::Regex;

/// A single parsed hunk from a unified diff.
pub struct ParsedHunk {
    /// Zero-based index of this hunk within the patch.
    pub index: usize,
    /// The raw hunk text including the `@@ ... @@` header line and all body lines.
    pub raw: String,
}

/// Regex for parsing hunk header: `@@ -oldStart,oldCount +newStart,newCount @@ section`
static HUNK_HEADER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(.*)$").unwrap());

/// Parse a unified-diff patch into its hunks. The file header lines
/// (`--- a/...`, `+++ b/...`) are NOT included in any hunk; only the
/// `@@`-delimited sections are returned. Returns hunks in file order.
pub fn parse_hunks(patch: &str) -> Result<Vec<ParsedHunk>, AppError> {
    let mut hunks = Vec::new();
    let mut current_lines: Option<Vec<&str>> = None;

    for line in patch.lines() {
        let is_hunk_header = line.starts_with("@@") && HUNK_HEADER_RE.is_match(line);
        if is_hunk_header {
            if let Some(lines) = current_lines.take() {
                hunks.push(ParsedHunk {
                    index: hunks.len(),
                    raw: lines.join("\n"),
                });
            }
            current_lines = Some(vec![line]);
        } else if let Some(ref mut lines) = current_lines {
            lines.push(line);
        }
    }

    if let Some(lines) = current_lines.take() {
        hunks.push(ParsedHunk {
            index: hunks.len(),
            raw: lines.join("\n"),
        });
    }

    Ok(hunks)
}

/// Invert a single hunk's text: swap added/removed lines and swap the
/// old/new ranges in the `@@` header. Context lines (leading space) are
/// preserved. The result is a valid single-hunk patch body.
pub fn invert_hunk(hunk_raw: &str) -> Result<String, AppError> {
    let (header, body) = hunk_raw.split_once('\n').unwrap_or((hunk_raw, ""));

    let caps = HUNK_HEADER_RE
        .captures(header)
        .ok_or_else(|| AppError::Internal(format!("Invalid hunk header: {header}")))?;

    let old_start = parse_capture(&caps, 1, "old start")?;
    let old_count = optional_capture(&caps, 2, "old count")?.unwrap_or(1);
    let new_start = parse_capture(&caps, 3, "new start")?;
    let new_count = optional_capture(&caps, 4, "new count")?.unwrap_or(1);
    let section = &caps[5];

    // Swap ranges: old ↔ new
    let inverted_header = format!("@@ -{new_start},{new_count} +{old_start},{old_count} @@{section}");

    // Swap +/- prefixes on body lines; context and `\ No newline` markers pass through.
    // After swapping, re-order within each change group so that `-` lines come before
    // `+` lines (standard unified-diff convention required by diffy).
    let body_lines: Vec<&str> = body.lines().collect();
    let mut inverted_lines: Vec<String> = Vec::new();
    let mut i = 0;
    while i < body_lines.len() {
        let line = body_lines[i];
        // `\ No newline at end of file` stays attached to its preceding line
        if line.starts_with("\\ ") {
            inverted_lines.push(line.to_string());
            i += 1;
            continue;
        }
        // Collect a contiguous change group (only + and - lines, no context)
        let mut deletions = Vec::new(); // lines that will be '-' after swap
        let mut insertions = Vec::new(); // lines that will be '+' after swap
        while i < body_lines.len() {
            let l = body_lines[i];
            if l.starts_with('\\') {
                // `\ No newline` belongs to the previous line — collect it with its group
                if !deletions.is_empty() {
                    deletions.push(l.to_string());
                } else {
                    insertions.push(l.to_string());
                }
                i += 1;
                continue;
            }
            if let Some(stripped) = l.strip_prefix('+') {
                // After swap this becomes a deletion
                deletions.push(format!("-{stripped}"));
                i += 1;
            } else if let Some(stripped) = l.strip_prefix('-') {
                // After swap this becomes an insertion
                insertions.push(format!("+{stripped}"));
                i += 1;
            } else {
                // Context line — end of change group
                break;
            }
        }
        // Emit deletions first, then insertions (standard diff order)
        inverted_lines.extend(deletions);
        inverted_lines.extend(insertions);

        // If we stopped at a context line, emit it
        if i < body_lines.len()
            && !body_lines[i].starts_with('+')
            && !body_lines[i].starts_with('-')
            && !body_lines[i].starts_with('\\')
        {
            inverted_lines.push(body_lines[i].to_string());
            i += 1;
        }
    }
    let inverted_body = inverted_lines.join("\n");

    if inverted_body.is_empty() {
        Ok(inverted_header)
    } else {
        Ok(format!("{inverted_header}\n{inverted_body}"))
    }
}

/// Apply a single (already-inverted) hunk to the given file content,
/// returning the new file content. Uses `diffy` to apply. The hunk must
/// be wrapped with synthetic file headers so diffy can parse it as a patch.
pub fn apply_inverted_hunk(file_content: &str, inverted_hunk_raw: &str, file_path: &str) -> Result<String, AppError> {
    // Ensure the synthetic patch ends with a newline so diffy correctly
    // interprets the last hunk body line as having a trailing newline.
    let hunk_text = if inverted_hunk_raw.ends_with('\n') {
        inverted_hunk_raw.to_string()
    } else {
        format!("{inverted_hunk_raw}\n")
    };
    let synthetic = format!("--- a/{file_path}\n+++ b/{file_path}\n{hunk_text}");
    let patch = diffy::Patch::from_str(&synthetic)
        .map_err(|e| AppError::Internal(format!("Failed to parse inverted hunk as patch: {e}")))?;
    diffy::apply(file_content, &patch).map_err(|e| AppError::Internal(format!("Failed to apply inverted hunk: {e}")))
}

/// Convenience: given a full forward patch, a hunk index, and the current
/// file content, produce the file content with ONLY that hunk reverted.
pub fn revert_single_hunk(
    forward_patch: &str,
    hunk_index: usize,
    file_content: &str,
    file_path: &str,
) -> Result<String, AppError> {
    let hunks = parse_hunks(forward_patch)?;
    let hunk = hunks.get(hunk_index).ok_or_else(|| {
        AppError::Internal(format!(
            "Hunk index {hunk_index} out of range (patch has {} hunks)",
            hunks.len()
        ))
    })?;
    let inverted = invert_hunk(&hunk.raw)?;
    apply_inverted_hunk(file_content, &inverted, file_path)
}

/// Invert an entire patch (all hunks) — used to replace the
/// `compute_inverse` placeholder. Preserves file headers but swaps
/// `---`/`+++` paths and inverts every hunk.
pub fn invert_patch(forward_patch: &str) -> Result<String, AppError> {
    let hunks = parse_hunks(forward_patch)?;
    let mut result = String::new();

    // Collect and swap ---/+++ paths from pre-hunk header lines
    let mut old_path: Option<String> = None;
    let mut new_path: Option<String> = None;

    for line in forward_patch.lines() {
        if line.starts_with("@@") && HUNK_HEADER_RE.is_match(line) {
            break;
        }
        if let Some(rest) = line.strip_prefix("--- ") {
            old_path = Some(rest.to_string());
        } else if let Some(rest) = line.strip_prefix("+++ ") {
            new_path = Some(rest.to_string());
        } else {
            // Pass through other header lines (diff, index, etc.)
            result.push_str(line);
            result.push('\n');
        }
    }

    // Write swapped headers
    if let (Some(old), Some(new)) = (old_path, new_path) {
        result.push_str(&format!("--- {new}\n+++ {old}\n"));
    }

    // Invert each hunk
    for hunk in &hunks {
        let inverted = invert_hunk(&hunk.raw)?;
        result.push_str(&inverted);
        result.push('\n');
    }

    // Trim trailing newline if original didn't have one
    if !forward_patch.ends_with('\n') && result.ends_with('\n') {
        result.pop();
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Parse a required regex capture group as `usize`.
fn parse_capture(caps: &regex::Captures<'_>, group: usize, label: &str) -> Result<usize, AppError> {
    caps[group]
        .parse()
        .map_err(|e| AppError::Internal(format!("Invalid {label} in hunk header: {e}")))
}

/// Parse an optional regex capture group as `usize`.
fn optional_capture(caps: &regex::Captures<'_>, group: usize, label: &str) -> Result<Option<usize>, AppError> {
    match caps.get(group) {
        Some(m) => Ok(Some(m.as_str().parse().map_err(|e| {
            AppError::Internal(format!("Invalid {label} in hunk header: {e}"))
        })?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Test 1: parse_hunks on a multi-hunk patch
    // -----------------------------------------------------------------------
    #[test]
    fn parse_hunks_returns_correct_count_and_indices() {
        let patch = "\
--- a/foo.txt
+++ b/foo.txt
@@ -1,2 +1,2 @@
-line1
+LINE1
 line2
@@ -3,2 +3,2 @@
 line3
-line4
+LINE4
";
        let hunks = parse_hunks(patch).unwrap();
        assert_eq!(hunks.len(), 2);
        assert_eq!(hunks[0].index, 0);
        assert_eq!(hunks[1].index, 1);
        assert!(hunks[0].raw.starts_with("@@ -1,2 +1,2 @@"));
        assert!(hunks[1].raw.starts_with("@@ -3,2 +3,2 @@"));
    }

    // -----------------------------------------------------------------------
    // Test 2: invert_hunk swaps ranges and body lines
    // -----------------------------------------------------------------------
    #[test]
    fn invert_hunk_swaps_ranges_and_lines() {
        let hunk = "@@ -1,1 +2,1 @@\n-old\n+new\n";
        let inverted = invert_hunk(hunk).unwrap();
        // Ranges swapped: -2,1 +1,1
        assert!(inverted.starts_with("@@ -2,1 +1,1 @@"));
        assert!(inverted.contains("-new"));
        assert!(inverted.contains("+old"));
        assert!(!inverted.contains("-old"));
        assert!(!inverted.contains("+new"));
    }

    // -----------------------------------------------------------------------
    // Test 3: apply_inverted_hunk round-trip
    // -----------------------------------------------------------------------
    #[test]
    fn apply_inverted_hunk_roundtrip() {
        let original = "hello\n";
        let modified = "goodbye\n";
        let forward_hunk = "@@ -1,1 +1,1 @@\n-hello\n+goodbye\n";
        let inverted = invert_hunk(forward_hunk).unwrap();
        let result = apply_inverted_hunk(modified, &inverted, "test.txt").unwrap();
        assert_eq!(result, original);
    }

    // -----------------------------------------------------------------------
    // Test 4: revert_single_hunk on a 2-hunk patch
    // -----------------------------------------------------------------------
    #[test]
    fn revert_single_hunk_targets_only_one_hunk() {
        let forward_patch = "\
--- a/test.txt
+++ b/test.txt
@@ -1,2 +1,2 @@
-line1
+LINE1
 line2
@@ -3,2 +3,2 @@
 line3
-line4
+LINE4
";
        let modified = "LINE1\nline2\nline3\nLINE4\n";

        // Revert hunk 0 only
        let result = revert_single_hunk(forward_patch, 0, modified, "test.txt").unwrap();
        assert_eq!(result, "line1\nline2\nline3\nLINE4\n");

        // Revert hunk 1 only
        let result = revert_single_hunk(forward_patch, 1, modified, "test.txt").unwrap();
        assert_eq!(result, "LINE1\nline2\nline3\nline4\n");
    }

    // -----------------------------------------------------------------------
    // Test 5: invert_patch full round-trip via diffy
    // -----------------------------------------------------------------------
    #[test]
    fn invert_patch_roundtrip_via_diffy() {
        let original = "hello\nworld\n";
        let forward = "\
--- a/test.txt
+++ b/test.txt
@@ -1,2 +1,2 @@
 hello
-world
+earth
";
        let inverse = invert_patch(forward).unwrap();

        // Apply forward → modified
        let forward_patch = diffy::Patch::from_str(forward).unwrap();
        let after_forward = diffy::apply(original, &forward_patch).unwrap();
        assert_eq!(after_forward, "hello\nearth\n");

        // Apply inverse → original
        let inverse_patch = diffy::Patch::from_str(&inverse).unwrap();
        let after_inverse = diffy::apply(&after_forward, &inverse_patch).unwrap();
        assert_eq!(after_inverse, original);
    }

    // -----------------------------------------------------------------------
    // Test 6: omitted counts default to 1
    // -----------------------------------------------------------------------
    #[test]
    fn omitted_counts_parse_and_invert_correctly() {
        let hunk = "@@ -1 +1 @@\n-old\n+new\n";
        let hunks = parse_hunks(&format!("--- a/f\n+++ b/f\n{hunk}")).unwrap();
        assert_eq!(hunks.len(), 1);
        let inverted = invert_hunk(&hunks[0].raw).unwrap();
        // Inverted emits explicit counts (both default to 1)
        assert!(inverted.starts_with("@@ -1,1 +1,1 @@"));
        assert!(inverted.contains("-new"));
        assert!(inverted.contains("+old"));
    }

    // -----------------------------------------------------------------------
    // Test 7: pure insertion inverts to pure deletion and applies
    // -----------------------------------------------------------------------
    #[test]
    fn pure_insertion_inverts_to_deletion_and_applies() {
        let hunk = "@@ -1,2 +1,3 @@\n line1\n+inserted\n line2\n";
        let inverted = invert_hunk(hunk).unwrap();
        assert!(inverted.contains("-inserted"));
        assert!(!inverted.contains("+inserted"));

        let original = "line1\nline2\n";
        let modified = "line1\ninserted\nline2\n";
        let result = apply_inverted_hunk(modified, &inverted, "test.txt").unwrap();
        assert_eq!(result, original);
    }

    // -----------------------------------------------------------------------
    // Test 8: context lines preserved in inversion
    // -----------------------------------------------------------------------
    #[test]
    fn context_lines_preserved_in_inversion() {
        let hunk = "@@ -1,3 +1,3 @@\n ctx1\n-old\n+new\n ctx2\n";
        let inverted = invert_hunk(hunk).unwrap();
        assert!(inverted.contains(" ctx1"));
        assert!(inverted.contains(" ctx2"));
        assert!(inverted.contains("-new"));
        assert!(inverted.contains("+old"));
        // Original +/- lines should not appear
        assert!(!inverted.contains("-old"));
        assert!(!inverted.contains("+new"));
    }
}
