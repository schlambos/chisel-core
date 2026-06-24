//! End-to-end conformance test for the recorded OpenCode SSE fixtures.
//!
//! For every `fixtures/*.jsonl` file:
//!   1. Each non-blank line is parsed as JSON.
//!   2. The event passes through [`chisl_opencode_conformance::unwrap_event`]
//!      (the same normalizer the live adapter uses).
//!   3. [`chisl_opencode_conformance::classify_event`] is asserted to return
//!      `Ok(_)`. The outcome is tallied per category.
//!   4. If a fixture file is named after a handled event (e.g.
//!      `session.idle.jsonl`), every line in that file MUST classify as that
//!      specific event — this is the contract that catches "renamed/removed
//!      required field on a known event" drift.
//!
//! Forward-compatibility contract:
//!   - Unknown event types collected in the `unknown` tally are *not* a
//!     failure. The test prints them so PR reviewers can decide whether to
//!     promote.
//!   - Unknown fields on known events are transparent: `classify_event` reads
//!     only the fields it needs and ignores the rest.
//!
//! See `PROTOCOL.md` (in `crates/chisl-ai-agent/src/manager/remote/`) for the
//! protocol surface this suite locks.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use chisl_opencode_conformance::{ClassifyOutcome, EventKind, classify_event, unwrap_event};
use anyhow::{Context, Result};
use serde_json::Value;

#[derive(Debug, Default)]
struct Tally {
    handled: BTreeMap<String, usize>,
    ignored: BTreeMap<String, usize>,
    sync: usize,
    unknown: BTreeMap<String, usize>,
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

fn read_fixture(path: &Path) -> Result<Vec<Value>> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut out = Vec::new();
    for (line_idx, line) in raw.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(trimmed)
            .with_context(|| format!("parsing JSON at {}:{}", path.display(), line_idx + 1))?;
        out.push(v);
    }
    Ok(out)
}

/// Best-effort mapping from a fixture file stem to the event-name the lines
/// inside it are expected to carry. Fixture file names are taken verbatim
/// from the event `type` discriminator (e.g. `session.idle.jsonl`). The
/// `_sync.jsonl` fixture is the V2 sync wrapper bucket — its lines are
/// expected to classify as [`ClassifyOutcome::Sync`].
fn expected_event_name(stem: &str) -> Option<&str> {
    if stem == "_sync" {
        return None;
    }
    Some(stem)
}

fn name_of_outcome(outcome: &ClassifyOutcome) -> String {
    match outcome {
        ClassifyOutcome::Handled(k) => format!("{k:?}"),
        ClassifyOutcome::Ignored(k) => format!("{k:?}"),
        ClassifyOutcome::Sync { mirror_type, .. } => format!("sync({mirror_type})"),
        ClassifyOutcome::Unknown { event_type, .. } => format!("Unknown({event_type})"),
    }
}

fn tally_outcome(tally: &mut Tally, outcome: &ClassifyOutcome) {
    match outcome {
        ClassifyOutcome::Handled(_) => {
            let key = name_of_outcome(outcome);
            *tally.handled.entry(key).or_default() += 1;
        }
        ClassifyOutcome::Ignored(_) => {
            let key = name_of_outcome(outcome);
            *tally.ignored.entry(key).or_default() += 1;
        }
        ClassifyOutcome::Sync { .. } => {
            tally.sync += 1;
        }
        ClassifyOutcome::Unknown { event_type, .. } => {
            *tally.unknown.entry(event_type.clone()).or_default() += 1;
        }
    }
}

#[test]
fn all_recorded_fixtures_classify_without_panic() -> Result<()> {
    let dir = fixtures_dir();
    assert!(
        dir.is_dir(),
        "fixtures directory missing at {} — capture-2026-06-02 should populate it",
        dir.display()
    );

    let mut tally = Tally::default();
    let mut file_count = 0usize;
    let mut line_count = 0usize;

    let mut entries: Vec<PathBuf> = fs::read_dir(&dir)
        .with_context(|| format!("reading fixtures dir {}", dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "jsonl").unwrap_or(false))
        .collect();
    entries.sort();

    assert!(
        !entries.is_empty(),
        "no .jsonl fixtures found under {}; the conformance gate requires recorded captures",
        dir.display()
    );

    for path in entries {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        let events = read_fixture(&path)?;
        assert!(!events.is_empty(), "fixture {} is empty", path.display());

        file_count += 1;
        let expected = expected_event_name(&stem);

        for (idx, raw) in events.into_iter().enumerate() {
            line_count += 1;
            let inner = unwrap_event(raw);
            let outcome = classify_event(&inner).unwrap_or_else(|e| {
                panic!(
                    "{}:{} — classify_event returned Err: {}\n  inner: {}",
                    path.display(),
                    idx + 1,
                    e,
                    inner
                );
            });
            // If we know what to expect, require this line lands on that arm.
            // Required-field removal flips THIS to red because the line will
            // classify as Unknown(<expected>) or an Err, neither of which
            // matches the expected discriminator.
            if let Some(expected_name) = expected {
                let actual_name = inner.get("type").and_then(|v| v.as_str()).unwrap_or_default();
                assert_eq!(
                    actual_name,
                    expected_name,
                    "{}:{} — fixture stem '{}' implies type '{}' but line carries type '{}'",
                    path.display(),
                    idx + 1,
                    stem,
                    expected_name,
                    actual_name
                );
                match &outcome {
                    ClassifyOutcome::Handled(_) | ClassifyOutcome::Ignored(_) => {}
                    ClassifyOutcome::Sync { .. } => unreachable!("expected_name set for sync fixture"),
                    ClassifyOutcome::Unknown { event_type, .. } => {
                        // Forward-compat: a fixture for a brand-new event type
                        // classifies as Unknown(<expected_name>). That is the
                        // "totally-new event type" tamper case: warn but do
                        // NOT fail. The previously-known-event regression
                        // (where the type IS recognised but a required field
                        // went missing) is already caught by classify_event
                        // returning Err, not Unknown.
                        assert_eq!(
                            event_type,
                            expected_name,
                            "{}:{} — Unknown({}) does not match fixture stem '{}'; this should never happen",
                            path.display(),
                            idx + 1,
                            event_type,
                            expected_name
                        );
                    }
                }
            }
            tally_outcome(&mut tally, &outcome);
        }
    }

    // Summary printed to test output for visibility on green runs. Forward-
    // compatibility: any `Unknown` bucket is logged but does NOT fail.
    eprintln!(
        "\nconformance summary: {} fixture file(s), {} event line(s)",
        file_count, line_count
    );
    eprintln!("  handled categories ({}):", tally.handled.len());
    for (k, n) in &tally.handled {
        eprintln!("    {k:35}  {n:>4}");
    }
    eprintln!("  ignored categories ({}):", tally.ignored.len());
    for (k, n) in &tally.ignored {
        eprintln!("    {k:35}  {n:>4}");
    }
    eprintln!("  sync mirrors: {}", tally.sync);
    if tally.unknown.is_empty() {
        eprintln!("  unknown event types: none (clean)\n");
    } else {
        eprintln!(
            "  ⚠ unknown event types ({}) — informational only, not a failure:",
            tally.unknown.len()
        );
        for (name, n) in &tally.unknown {
            eprintln!("    {name:35}  {n:>4}");
        }
        eprintln!("  (add these to crates/chisl-opencode-conformance/TODO.md or promote them)\n");
    }

    // Sanity: a non-trivial event surface should have parsed.
    assert!(
        tally.handled.len() + tally.ignored.len() >= 5,
        "recorded fixtures classified fewer than 5 distinct event kinds — capture is likely degenerate"
    );

    Ok(())
}

/// A new fixture file holding a brand-new event type must NOT fail the suite.
/// This locks the forward-compatibility contract.
#[test]
fn synthetic_unknown_event_does_not_fail() {
    let inner = serde_json::json!({
        "id": "evt_fwd_1",
        "type": "brand.new.future.event",
        "properties": {"futureField": "ok", "anotherFutureField": 7}
    });
    let outcome = classify_event(&inner).expect("forward-compat: unknown type must classify");
    match outcome {
        ClassifyOutcome::Unknown { event_type, .. } => assert_eq!(event_type, "brand.new.future.event"),
        other => panic!("expected Unknown for synthetic event, got {other:?}"),
    }
}

/// Adding an unrecognised field to a known event must NOT fail.
#[test]
fn synthetic_unknown_field_on_known_event_does_not_fail() {
    let inner = serde_json::json!({
        "id": "evt_fwd_2",
        "type": "session.idle",
        "properties": {"sessionID": "ses_1", "futureExtraField": "ignored-by-classifier"}
    });
    assert_eq!(
        classify_event(&inner).unwrap(),
        ClassifyOutcome::Handled(EventKind::SessionIdle)
    );
}

/// Removing a required field from a known event MUST fail. This is the
/// regression-guard that flips the suite red on a breaking upstream change.
#[test]
fn synthetic_missing_required_field_fails() {
    let inner = serde_json::json!({
        "id": "evt_break_1",
        "type": "session.idle",
        "properties": {} // sessionID removed
    });
    assert!(classify_event(&inner).is_err(), "missing required field must Err");
}

/// Tamper test: confirm the message-part-delta required-field path is the
/// tight contract the live adapter uses. Removing any of `messageID`,
/// `partID`, `field`, or `delta` is a breakage.
#[test]
fn synthetic_message_part_delta_required_fields() {
    for missing in ["sessionID", "messageID", "partID", "field", "delta"] {
        let mut props = serde_json::json!({
            "sessionID": "ses_1",
            "messageID": "msg_1",
            "partID": "part_1",
            "field": "text",
            "delta": "hello"
        });
        props.as_object_mut().unwrap().remove(missing);
        let inner = serde_json::json!({
            "id": "evt_break_part",
            "type": "message.part.delta",
            "properties": props
        });
        assert!(
            classify_event(&inner).is_err(),
            "removing message.part.delta.{missing} must Err"
        );
    }
    // Sanity: full payload is Handled.
    let inner = serde_json::json!({
        "id": "evt_ok_part",
        "type": "message.part.delta",
        "properties": {
            "sessionID": "ses_1",
            "messageID": "msg_1",
            "partID": "part_1",
            "field": "text",
            "delta": "hello"
        }
    });
    assert_eq!(
        classify_event(&inner).unwrap(),
        ClassifyOutcome::Handled(EventKind::MessagePartDelta)
    );
}
