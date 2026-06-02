//! OpenCode `/question` flow (M09).
//!
//! Distinct from `permission.asked` (which gates tool execution), the question
//! flow is the model asking the *user* a clarifying question via the `ask` tool.
//! The server surface (verified against opencode 1.15.11, `/doc`):
//!
//! - SSE `question.asked` → [`QuestionRequest`]:
//!   `{ id: "que…", sessionID: "ses…", questions: QuestionInfo[], tool }`
//!   where each `QuestionInfo` is
//!   `{ question, header, options: {label, description}[], multiple?, custom? }`.
//! - `POST /question/{requestID}/reply` body `{ answers: string[][] }` — one
//!   inner array of selected option **labels** per question, in order.
//! - `POST /question/{requestID}/reject` — no body; closes the question
//!   unanswered (typically aborts the turn).
//! - `GET /question` → `QuestionRequest[]` (pending list, for reconnect
//!   backfill — not yet wired; see the M09 plan §3.3).
//!
//! ## Mapping onto the existing Approvals queue
//!
//! Rather than introduce a parallel UI, each [`QuestionInfo`] is mapped to one
//! [`Confirmation`] so it rides the exact same Approvals-tab path that
//! permission prompts already use. Option `label`s become the radio choices
//! (`value == label`); a synthetic **Reject** option is always appended so even
//! a freeform/option-less question can be dismissed without stalling the turn.
//!
//! A request with multiple questions emits multiple cards; answers are buffered
//! in [`PendingQuestion`] until every question is answered, then a single
//! `reply` POST carries the full `answers` matrix.
//!
//! ## P1.2a (D4) answer matrix
//!
//! - `multiple` (multi-select) — the renderer presents a chip-style
//!   multi-select UI. We add a single synthetic `__question_all__` option
//!   that, when selected, accepts ALL provided options (matching
//!   OpenCode's `multiple` semantics). The renderer can also submit a
//!   hand-picked subset; `record_multi` accepts a list of labels.
//! - `custom` (freeform) — the renderer presents a freeform text input
//!   with optional `secret: true` password mask. The answer is a single
//!   string in the `answers` matrix and MUST NOT appear in logs,
//!   telemetry, or i18n strings (P1.2a security constraint). We tag the
//!   `Confirmation.options[0].params` with `kind: "freeform"` and
//!   `secret: <bool>` so the renderer can pick the right input flavour.
//! - Plain single-select — unchanged from M09; the provided labels are
//!   radio choices and a Reject sentinel is appended.

use aionui_common::{Confirmation, ConfirmationOption};
use serde_json::{Value, json};

/// Synthetic option value used to reject a question from the Approvals card.
pub const QUESTION_REJECT_VALUE: &str = "__question_reject__";
/// Plain-English reject label (rendered via `t(label, {defaultValue: label})`
/// on the renderer, matching how permission options label themselves).
const QUESTION_REJECT_LABEL: &str = "Reject";
/// Synthetic `call_id` prefix that routes a confirmation back to the question
/// flow in `agent.rs::confirm()`.
const CALL_ID_PREFIX: &str = "question-";

/// P1.2a (D4): multi-select "accept all" sentinel. When the user picks this
/// option we treat the answer as "every provided option" — matches OpenCode's
/// `multiple` semantics where submitting `[]` would be ambiguous.
pub const QUESTION_ALL_VALUE: &str = "__question_all__";
/// P1.2a (D4): freeform-text sentinel. The renderer substitutes the typed
/// value at confirm time. Stamped into the `value` field of the
/// `__question_freeform__` option so the round-trip is unambiguous.
pub const QUESTION_FREEFORM_VALUE: &str = "__question_freeform__";

/// One question within a [`ParsedQuestionRequest`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedQuestion {
    pub header: String,
    pub question: String,
    /// `(label, description)` choices offered by the server.
    pub options: Vec<(String, String)>,
    pub multiple: bool,
    pub custom: bool,
    /// P1.2a (D4): if `custom: true` and this is also `true`, the renderer
    /// MUST render a password-masked input and MUST NOT log/telemetry/i18n
    /// the typed value.
    pub secret: bool,
}

/// A parsed `question.asked` payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedQuestionRequest {
    pub request_id: String,
    pub session_id: Option<String>,
    pub questions: Vec<ParsedQuestion>,
}

/// A buffered question request accumulating per-question answers across the
/// (possibly multiple) Approvals cards it produced.
#[derive(Debug, Clone)]
pub struct PendingQuestion {
    pub request_id: String,
    pub session_id: Option<String>,
    /// One slot per question; `None` until answered, then the selected labels.
    answers: Vec<Option<Vec<String>>>,
}

impl PendingQuestion {
    pub fn new(req: &ParsedQuestionRequest) -> Self {
        Self {
            request_id: req.request_id.clone(),
            session_id: req.session_id.clone(),
            answers: vec![None; req.questions.len()],
        }
    }

    /// Record the answer for question `index`. Out-of-range indices are
    /// ignored. Returns `true` if the slot was newly filled.
    pub fn record(&mut self, index: usize, labels: Vec<String>) -> bool {
        match self.answers.get_mut(index) {
            Some(slot) => {
                let was_empty = slot.is_none();
                *slot = Some(labels);
                was_empty
            }
            None => false,
        }
    }

    /// Whether every question has an answer.
    pub fn is_complete(&self) -> bool {
        !self.answers.is_empty() && self.answers.iter().all(Option::is_some)
    }

    /// The full `answers` matrix for the reply body. Unanswered slots collapse
    /// to an empty array (only meaningful once [`Self::is_complete`]).
    pub fn collected(&self) -> Vec<Vec<String>> {
        self.answers
            .iter()
            .map(|slot| slot.clone().unwrap_or_default())
            .collect()
    }
}

/// Parse a `question.asked` event's `properties` (a `QuestionRequest`).
/// Returns `None` when the required `id` / `questions` are missing.
pub fn parse_question_request(props: &Value) -> Option<ParsedQuestionRequest> {
    let request_id = props.get("id").and_then(|v| v.as_str())?.to_string();
    let session_id = props.get("sessionID").and_then(|v| v.as_str()).map(String::from);
    let questions_raw = props.get("questions").and_then(|v| v.as_array())?;

    let questions: Vec<ParsedQuestion> = questions_raw
        .iter()
        .map(|q| {
            let header = q.get("header").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let question = q.get("question").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let options = q
                .get("options")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|o| {
                            let label = o.get("label").and_then(|v| v.as_str())?.to_string();
                            let description = o.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            Some((label, description))
                        })
                        .collect()
                })
                .unwrap_or_default();
            let multiple = q.get("multiple").and_then(|v| v.as_bool()).unwrap_or(false);
            let custom = q.get("custom").and_then(|v| v.as_bool()).unwrap_or(false);
            // P1.2a (D4): secret flag only meaningful when `custom: true`.
            // We never log / telemetry the typed value either way; the flag
            // just tells the renderer to render a password-masked input.
            let secret = q.get("secret").and_then(|v| v.as_bool()).unwrap_or(false) && custom;
            ParsedQuestion {
                header,
                question,
                options,
                multiple,
                custom,
                secret,
            }
        })
        .collect();

    if questions.is_empty() {
        return None;
    }

    Some(ParsedQuestionRequest {
        request_id,
        session_id,
        questions,
    })
}

/// Synthesize the Approvals `call_id` for question `index` of `request_id`.
pub fn question_call_id(request_id: &str, index: usize) -> String {
    format!("{CALL_ID_PREFIX}{request_id}-{index}")
}

/// Whether a `call_id` belongs to the question flow.
pub fn is_question_call_id(call_id: &str) -> bool {
    call_id.starts_with(CALL_ID_PREFIX)
}

/// Decode `(request_id, index)` from a question `call_id`. Splits on the LAST
/// `-` so request ids that themselves contain `-` round-trip correctly.
pub fn parse_question_call_id(call_id: &str) -> Option<(String, usize)> {
    let rest = call_id.strip_prefix(CALL_ID_PREFIX)?;
    let (request_id, index_str) = rest.rsplit_once('-')?;
    if request_id.is_empty() {
        return None;
    }
    let index = index_str.parse::<usize>().ok()?;
    Some((request_id.to_string(), index))
}

/// Build one [`Confirmation`] per question so they queue into the Approvals
/// tab. Each option becomes a radio choice keyed by its label; a Reject option
/// is always appended. P1.2a (D4): also emits a freeform option when
/// `custom: true` and an "all" option when `multiple: true`.
pub fn build_question_confirmations(req: &ParsedQuestionRequest) -> Vec<Confirmation> {
    req.questions
        .iter()
        .enumerate()
        .map(|(index, q)| {
            let call_id = question_call_id(&req.request_id, index);

            // Surface each option's description inline since the radio only
            // shows labels. Append a hint for multi/custom questions.
            let mut description = q.question.clone();
            for (label, desc) in &q.options {
                if !desc.is_empty() {
                    description.push_str(&format!("\n• {label} — {desc}"));
                }
            }
            if q.multiple {
                description.push_str("\n(server allows multiple answers; pick one, several, or \"All\")");
            }
            if q.custom {
                let secret_hint = if q.secret { " (secret)" } else { "" };
                description.push_str(&format!(
                    "\n(server allows a custom answer{secret_hint}; type your own or pick the closest option)"
                ));
            }

            let mut options: Vec<ConfirmationOption> = q
                .options
                .iter()
                .map(|(label, _desc)| ConfirmationOption {
                    label: label.clone(),
                    value: Value::String(label.clone()),
                    params: None,
                })
                .collect();
            // P1.2a (D4): freeform-text input. Renderer substitutes the
            // typed value at confirm time; the typed value MUST NEVER be
            // log/telemetry/i18n'd. The `kind`/`secret` flags in `params`
            // let the renderer pick the right input flavour.
            if q.custom {
                let mut params = std::collections::HashMap::new();
                params.insert("kind".to_string(), "freeform".to_string());
                if q.secret {
                    params.insert("secret".to_string(), "true".to_string());
                }
                options.push(ConfirmationOption {
                    label: "Type a custom answer".to_string(),
                    value: Value::String(QUESTION_FREEFORM_VALUE.to_string()),
                    params: Some(params),
                });
            }
            // P1.2a (D4): multi-select "All" option. Selecting this
            // commits every provided label; a hand-picked subset is also
            // supported via the same multi-select UI.
            if q.multiple && !q.options.is_empty() {
                options.push(ConfirmationOption {
                    label: "All of the above".to_string(),
                    value: Value::String(QUESTION_ALL_VALUE.to_string()),
                    params: None,
                });
            }
            options.push(ConfirmationOption {
                label: QUESTION_REJECT_LABEL.to_string(),
                value: Value::String(QUESTION_REJECT_VALUE.to_string()),
                params: None,
            });

            let title = if q.header.is_empty() {
                "OpenCode question".to_string()
            } else {
                q.header.clone()
            };

            Confirmation {
                id: call_id.clone(),
                call_id,
                title: Some(title),
                action: Some("question".to_string()),
                description,
                // Intentionally `None`: a non-empty `command_type` renders as a
                // code block in the Approvals card, which is wrong for a
                // clarifying question.
                command_type: None,
                options,
                session_id: req.session_id.clone(),
                parent_session_id: None,
            }
        })
        .collect()
}

/// `(url, body)` for `POST /question/{requestID}/reply`.
pub fn build_question_reply_request(base_url: &str, request_id: &str, answers: &[Vec<String>]) -> (String, Value) {
    let url = format!("{base_url}/question/{request_id}/reply");
    let body = json!({ "answers": answers });
    (url, body)
}

/// URL for `POST /question/{requestID}/reject`.
pub fn build_question_reject_url(base_url: &str, request_id: &str) -> String {
    format!("{base_url}/question/{request_id}/reject")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_props() -> Value {
        json!({
            "id": "que_abc",
            "sessionID": "ses_1",
            "questions": [
                {
                    "header": "DB choice",
                    "question": "Which database should I use?",
                    "options": [
                        { "label": "Postgres", "description": "Relational, robust" },
                        { "label": "SQLite", "description": "Embedded, simple" }
                    ],
                    "multiple": false,
                    "custom": true
                }
            ],
            "tool": { "messageID": "msg_1", "callID": "call_1" }
        })
    }

    #[test]
    fn parses_full_request() {
        let req = parse_question_request(&sample_props()).expect("should parse");
        assert_eq!(req.request_id, "que_abc");
        assert_eq!(req.session_id.as_deref(), Some("ses_1"));
        assert_eq!(req.questions.len(), 1);
        let q = &req.questions[0];
        assert_eq!(q.header, "DB choice");
        assert_eq!(q.options.len(), 2);
        assert_eq!(q.options[0], ("Postgres".to_string(), "Relational, robust".to_string()));
        assert!(q.custom);
        assert!(!q.multiple);
        // secret flag only meaningful when custom: true; no `secret` here.
        assert!(!q.secret);
    }

    #[test]
    fn parses_secret_flag_only_when_custom_is_true() {
        let props = json!({
            "id": "que_secret",
            "sessionID": "ses_1",
            "questions": [{
                "header": "Token?",
                "question": "Paste a token",
                "options": [],
                "multiple": false,
                "custom": true,
                "secret": true
            }]
        });
        let req = parse_question_request(&props).expect("should parse");
        let q = &req.questions[0];
        assert!(q.custom);
        assert!(q.secret, "secret:true + custom:true must land on ParsedQuestion.secret");
        // Standalone secret without custom MUST NOT flip the bit.
        let props2 = json!({
            "id": "que_nosecret",
            "sessionID": "ses_1",
            "questions": [{
                "header": "Pick",
                "question": "Pick one",
                "options": [{ "label": "A", "description": "" }],
                "multiple": false,
                "custom": false,
                "secret": true
            }]
        });
        let req2 = parse_question_request(&props2).expect("should parse");
        assert!(!req2.questions[0].secret);
    }

    #[test]
    fn rejects_missing_id_or_questions() {
        assert!(parse_question_request(&json!({ "questions": [] })).is_none());
        assert!(parse_question_request(&json!({ "id": "que_x", "questions": [] })).is_none());
        assert!(parse_question_request(&json!({ "id": "que_x" })).is_none());
    }

    #[test]
    fn call_id_round_trips_including_hyphenated_request_ids() {
        let cid = question_call_id("que_abc", 2);
        assert_eq!(cid, "question-que_abc-2");
        assert!(is_question_call_id(&cid));
        assert_eq!(parse_question_call_id(&cid), Some(("que_abc".to_string(), 2)));

        // request id containing hyphens must still round-trip (split on last '-').
        let cid2 = question_call_id("que-with-dashes", 0);
        assert_eq!(parse_question_call_id(&cid2), Some(("que-with-dashes".to_string(), 0)));

        assert!(parse_question_call_id("shell-123").is_none());
        assert!(parse_question_call_id("question-").is_none());
        assert!(parse_question_call_id("question-que_x-notanum").is_none());
    }

    #[test]
    fn builds_one_confirmation_per_question_with_reject() {
        let req = parse_question_request(&sample_props()).unwrap();
        let confs = build_question_confirmations(&req);
        assert_eq!(confs.len(), 1);
        let c = &confs[0];
        assert_eq!(c.call_id, "question-que_abc-0");
        assert_eq!(c.title.as_deref(), Some("DB choice"));
        assert_eq!(c.command_type, None);
        assert_eq!(c.session_id.as_deref(), Some("ses_1"));
        // 2 real options + freeform + Reject = 4
        assert_eq!(c.options.len(), 4);
        assert_eq!(c.options[0].value, json!("Postgres"));
        // freeform option is appended before the Reject.
        assert_eq!(c.options[2].value, json!(QUESTION_FREEFORM_VALUE));
        assert_eq!(
            c.options[2]
                .params
                .as_ref()
                .and_then(|p| p.get("kind"))
                .map(String::as_str),
            Some("freeform")
        );
        // No secret flag on this props.
        assert!(c.options[2].params.as_ref().and_then(|p| p.get("secret")).is_none());
        assert_eq!(c.options[3].value, json!(QUESTION_REJECT_VALUE));
        // Option descriptions surface in the body.
        assert!(c.description.contains("Relational, robust"));
        // custom hint present.
        assert!(c.description.contains("custom answer"));
    }

    #[test]
    fn builds_freeform_option_with_secret_flag() {
        let props = json!({
            "id": "que_secret",
            "sessionID": "ses_1",
            "questions": [{
                "header": "Token?",
                "question": "Paste a token",
                "options": [],
                "multiple": false,
                "custom": true,
                "secret": true
            }]
        });
        let req = parse_question_request(&props).unwrap();
        let confs = build_question_confirmations(&req);
        assert_eq!(confs.len(), 1);
        let c = &confs[0];
        // 0 real options + freeform + Reject = 2
        assert_eq!(c.options.len(), 2);
        let freeform = c
            .options
            .iter()
            .find(|o| o.value == json!(QUESTION_FREEFORM_VALUE))
            .expect("freeform option present");
        let params = freeform.params.as_ref().expect("params present");
        assert_eq!(params.get("kind").map(String::as_str), Some("freeform"));
        assert_eq!(params.get("secret").map(String::as_str), Some("true"));
        // secret hint in the description body.
        assert!(c.description.contains("(secret)"));
    }

    #[test]
    fn builds_multiple_select_with_all_option() {
        let props = json!({
            "id": "que_multi",
            "sessionID": "ses_1",
            "questions": [{
                "header": "Tools?",
                "question": "Which tools?",
                "options": [
                    { "label": "bash", "description": "run shell" },
                    { "label": "read", "description": "read file" },
                    { "label": "write", "description": "write file" }
                ],
                "multiple": true,
                "custom": false
            }]
        });
        let req = parse_question_request(&props).unwrap();
        let confs = build_question_confirmations(&req);
        assert_eq!(confs.len(), 1);
        let c = &confs[0];
        // 3 real + All + Reject = 5
        assert_eq!(c.options.len(), 5);
        let all = c
            .options
            .iter()
            .find(|o| o.value == json!(QUESTION_ALL_VALUE))
            .expect("all option present");
        assert_eq!(all.label, "All of the above");
        // multi-select hint in the description.
        assert!(c.description.contains("multiple answers"));
    }

    #[test]
    fn all_value_present_in_options() {
        // Guard against an accidental rename of the multi-select sentinel.
        assert!(!QUESTION_ALL_VALUE.is_empty());
        assert!(!QUESTION_FREEFORM_VALUE.is_empty());
    }

    #[test]
    fn reply_request_shape_matches_server() {
        let answers = vec![vec!["Postgres".to_string()]];
        let (url, body) = build_question_reply_request("http://h:4096", "que_abc", &answers);
        assert_eq!(url, "http://h:4096/question/que_abc/reply");
        assert_eq!(body, json!({ "answers": [["Postgres"]] }));
    }

    #[test]
    fn reject_url_shape() {
        assert_eq!(
            build_question_reject_url("http://h:4096", "que_abc"),
            "http://h:4096/question/que_abc/reject"
        );
    }

    #[test]
    fn pending_question_completion_lifecycle() {
        let props = json!({
            "id": "que_multi",
            "sessionID": "ses_1",
            "questions": [
                { "header": "A", "question": "q1", "options": [{ "label": "x", "description": "" }] },
                { "header": "B", "question": "q2", "options": [{ "label": "y", "description": "" }] }
            ]
        });
        let req = parse_question_request(&props).unwrap();
        let mut pending = PendingQuestion::new(&req);
        assert!(!pending.is_complete());
        assert!(pending.record(0, vec!["x".to_string()]));
        assert!(!pending.is_complete());
        assert!(pending.record(1, vec!["y".to_string()]));
        assert!(pending.is_complete());
        assert_eq!(pending.collected(), vec![vec!["x".to_string()], vec!["y".to_string()]]);
        // out-of-range record is a no-op.
        assert!(!pending.record(9, vec!["z".to_string()]));
    }
}
