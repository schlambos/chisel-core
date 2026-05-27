//! Render a compact, gitignore-aware listing of files under the project
//! root. Used to seed the model's system context so it doesn't have to
//! guess at the project layout (and won't confabulate that paths are
//! missing).

use std::path::Path;

use ignore::WalkBuilder;

/// Maximum number of file entries to include. The model only needs a
/// scaffold of the layout, not every file. Anything above this is
/// truncated with a marker.
const DEFAULT_MAX_ENTRIES: usize = 400;

/// Render a flat, sorted list of files relative to `root`. Respects
/// `.gitignore`. Returns one path per line, leading `./`. Truncates at
/// `max_entries` with a marker so the model knows there's more.
pub fn render_project_tree(root: &Path, max_entries: usize) -> String {
    let max_entries = max_entries.max(1);
    let mut entries: Vec<String> = Vec::new();
    let mut truncated = false;

    let walker = WalkBuilder::new(root)
        .hidden(false)
        .follow_links(false)
        .require_git(false)
        .build();

    for dent in walker {
        let entry = match dent {
            Ok(e) => e,
            Err(_) => continue,
        };
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        let rel = match entry.path().strip_prefix(root) {
            Ok(p) => p,
            Err(_) => continue,
        };
        if rel.as_os_str().is_empty() {
            continue;
        }
        let display = format!("./{}", rel.to_string_lossy());
        entries.push(display);
        if entries.len() > max_entries * 4 {
            // Hard ceiling on walker work in pathological repos; we sort
            // and truncate properly below.
            truncated = true;
            break;
        }
    }

    entries.sort();
    if entries.len() > max_entries {
        entries.truncate(max_entries);
        truncated = true;
    }

    if entries.is_empty() {
        return String::from("(project is empty)");
    }

    let mut out = entries.join("\n");
    if truncated {
        out.push_str(&format!(
            "\n… (listing truncated at {} entries; use list_dir for deeper subtrees)",
            max_entries
        ));
    }
    out
}

/// Convenience wrapper using the default cap.
pub fn render_project_tree_default(root: &Path) -> String {
    render_project_tree(root, DEFAULT_MAX_ENTRIES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp() -> (TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        (dir, root)
    }

    #[test]
    fn empty_project_renders_placeholder() {
        let (_g, root) = tmp();
        assert_eq!(render_project_tree(&root, 100), "(project is empty)");
    }

    #[test]
    fn lists_files_sorted_and_prefixed() {
        let (_g, root) = tmp();
        std::fs::create_dir(root.join("src")).unwrap();
        std::fs::write(root.join("Cargo.toml"), "").unwrap();
        std::fs::write(root.join("src/main.rs"), "").unwrap();
        std::fs::write(root.join("src/lib.rs"), "").unwrap();
        let out = render_project_tree(&root, 100);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines, vec!["./Cargo.toml", "./src/lib.rs", "./src/main.rs"]);
    }

    #[test]
    fn respects_gitignore() {
        let (_g, root) = tmp();
        std::fs::write(root.join(".gitignore"), "ignored.txt\n").unwrap();
        std::fs::write(root.join("kept.txt"), "").unwrap();
        std::fs::write(root.join("ignored.txt"), "").unwrap();
        let out = render_project_tree(&root, 100);
        assert!(out.contains("./kept.txt"));
        assert!(!out.contains("./ignored.txt"));
    }

    #[test]
    fn truncates_with_marker() {
        let (_g, root) = tmp();
        for i in 0..20 {
            std::fs::write(root.join(format!("f{i:02}.txt")), "").unwrap();
        }
        let out = render_project_tree(&root, 5);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 6);
        assert!(lines[5].contains("truncated"));
    }
}
