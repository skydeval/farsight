//! The published lexicons (see `docs/design/api.md`), embedded, and a
//! validator for the subset of lexicon types they use. The harnesses parse
//! every response against them; unit tests check the files load and every
//! `ref` resolves.

use std::collections::HashMap;

use serde_json::Value;

/// `(nsid, json)` of every lexicon file.
pub const FILES: &[(&str, &str)] = &[
    (
        "app.nearhorizon.farsight.defs",
        include_str!("../../../lexicons/app/nearhorizon/farsight/defs.json"),
    ),
    (
        "app.nearhorizon.farsight.query.getIncomingBlocks",
        include_str!("../../../lexicons/app/nearhorizon/farsight/query/getIncomingBlocks.json"),
    ),
    (
        "app.nearhorizon.farsight.query.getIncomingListBlocks",
        include_str!("../../../lexicons/app/nearhorizon/farsight/query/getIncomingListBlocks.json"),
    ),
    (
        "app.nearhorizon.farsight.query.getListsNaming",
        include_str!("../../../lexicons/app/nearhorizon/farsight/query/getListsNaming.json"),
    ),
    (
        "app.nearhorizon.farsight.query.getListMembers",
        include_str!("../../../lexicons/app/nearhorizon/farsight/query/getListMembers.json"),
    ),
    (
        "app.nearhorizon.farsight.query.checkBlocks",
        include_str!("../../../lexicons/app/nearhorizon/farsight/query/checkBlocks.json"),
    ),
    (
        "app.nearhorizon.farsight.query.getStats",
        include_str!("../../../lexicons/app/nearhorizon/farsight/query/getStats.json"),
    ),
    (
        "app.nearhorizon.farsight.query.getBackfillStatus",
        include_str!("../../../lexicons/app/nearhorizon/farsight/query/getBackfillStatus.json"),
    ),
    (
        "app.nearhorizon.farsight.admin.requestBackfill",
        include_str!("../../../lexicons/app/nearhorizon/farsight/admin/requestBackfill.json"),
    ),
    (
        "app.nearhorizon.farsight.admin.listErrors",
        include_str!("../../../lexicons/app/nearhorizon/farsight/admin/listErrors.json"),
    ),
    (
        "app.nearhorizon.farsight.admin.restartFirehose",
        include_str!("../../../lexicons/app/nearhorizon/farsight/admin/restartFirehose.json"),
    ),
    (
        "app.nearhorizon.farsight.admin.pauseSweep",
        include_str!("../../../lexicons/app/nearhorizon/farsight/admin/pauseSweep.json"),
    ),
    (
        "app.nearhorizon.farsight.admin.startRepair",
        include_str!("../../../lexicons/app/nearhorizon/farsight/admin/startRepair.json"),
    ),
    (
        "app.nearhorizon.farsight.admin.pauseRepair",
        include_str!("../../../lexicons/app/nearhorizon/farsight/admin/pauseRepair.json"),
    ),
    (
        "app.nearhorizon.farsight.admin.cancelRepair",
        include_str!("../../../lexicons/app/nearhorizon/farsight/admin/cancelRepair.json"),
    ),
    (
        "app.nearhorizon.farsight.admin.createApiKey",
        include_str!("../../../lexicons/app/nearhorizon/farsight/admin/createApiKey.json"),
    ),
    (
        "app.nearhorizon.farsight.admin.revokeApiKey",
        include_str!("../../../lexicons/app/nearhorizon/farsight/admin/revokeApiKey.json"),
    ),
];

/// The loaded lexicon set.
#[derive(Debug, Clone)]
pub struct Lexicons {
    docs: HashMap<String, Value>,
}

impl Default for Lexicons {
    fn default() -> Self {
        Lexicons::load()
    }
}

/// A validation failure: JSON path and what was wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// `$.a.b[2]`.
    pub path: String,
    /// The problem.
    pub problem: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.path, self.problem)
    }
}

impl Lexicons {
    /// Parses the embedded files.
    pub fn load() -> Lexicons {
        let docs = FILES
            .iter()
            .map(|(id, text)| {
                let v: Value = serde_json::from_str(text).expect("embedded lexicon parses");
                ((*id).to_owned(), v)
            })
            .collect();
        Lexicons { docs }
    }

    /// The `main` def of `nsid`.
    pub fn main(&self, nsid: &str) -> Option<&Value> {
        self.docs.get(nsid)?.get("defs")?.get("main")
    }

    fn resolve(&self, base: &str, r: &str) -> Option<(String, &Value)> {
        let (nsid, name) = match r.split_once('#') {
            Some(("", name)) => (base.to_owned(), name),
            Some((nsid, name)) => (nsid.to_owned(), name),
            None => (r.to_owned(), "main"),
        };
        let def = self.docs.get(&nsid)?.get("defs")?.get(name)?;
        Some((nsid, def))
    }

    /// Every `ref` target that does not resolve (unit test of the files).
    pub fn dangling_refs(&self) -> Vec<String> {
        fn walk(l: &Lexicons, base: &str, v: &Value, out: &mut Vec<String>) {
            match v {
                Value::Object(m) => {
                    if m.get("type").and_then(Value::as_str) == Some("ref") {
                        if let Some(r) = m.get("ref").and_then(Value::as_str) {
                            if l.resolve(base, r).is_none() {
                                out.push(format!("{base}: {r}"));
                            }
                        }
                    }
                    for x in m.values() {
                        walk(l, base, x, out);
                    }
                }
                Value::Array(a) => a.iter().for_each(|x| walk(l, base, x, out)),
                _ => {}
            }
        }
        let mut out = Vec::new();
        for (id, doc) in &self.docs {
            walk(self, id, doc, &mut out);
        }
        out
    }

    /// Validates a response body against `nsid`'s output schema.
    pub fn validate_output(&self, nsid: &str, body: &Value) -> Vec<Violation> {
        let mut out = Vec::new();
        match self
            .main(nsid)
            .and_then(|m| m.get("output"))
            .and_then(|o| o.get("schema"))
        {
            Some(schema) => self.check(nsid, schema, body, "$", &mut out),
            None => out.push(Violation {
                path: "$".into(),
                problem: format!("no output schema for {nsid}"),
            }),
        }
        out
    }

    /// Validates an error body: `{ error, message }` with an error name
    /// the method declares.
    pub fn validate_error(&self, nsid: &str, body: &Value) -> Vec<Violation> {
        let mut out = Vec::new();
        let name = body.get("error").and_then(Value::as_str);
        if name.is_none() || body.get("message").and_then(Value::as_str).is_none() {
            out.push(Violation {
                path: "$".into(),
                problem: "error body must be {error, message}".into(),
            });
        }
        if let Some(n) = name {
            let declared = self
                .main(nsid)
                .and_then(|m| m.get("errors"))
                .and_then(Value::as_array)
                .is_some_and(|e| {
                    e.iter()
                        .any(|x| x.get("name").and_then(Value::as_str) == Some(n))
                });
            if !declared {
                out.push(Violation {
                    path: "$.error".into(),
                    problem: format!("{n} is not declared by {nsid}"),
                });
            }
        }
        out
    }

    fn check(&self, base: &str, schema: &Value, v: &Value, path: &str, out: &mut Vec<Violation>) {
        let mut bad = |problem: String| {
            out.push(Violation {
                path: path.to_owned(),
                problem,
            })
        };
        let ty = schema.get("type").and_then(Value::as_str).unwrap_or("");
        match ty {
            "ref" => {
                let r = schema.get("ref").and_then(Value::as_str).unwrap_or("");
                match self.resolve(base, r) {
                    Some((nb, def)) => {
                        let nb = nb.clone();
                        self.check(&nb, def, v, path, out)
                    }
                    None => bad(format!("unresolvable ref {r}")),
                }
            }
            "object" => {
                let Some(m) = v.as_object() else {
                    return bad("expected object".into());
                };
                let props = schema.get("properties").and_then(Value::as_object);
                if let Some(req) = schema.get("required").and_then(Value::as_array) {
                    for r in req.iter().filter_map(Value::as_str) {
                        if !m.contains_key(r) {
                            out.push(Violation {
                                path: path.to_owned(),
                                problem: format!("missing required field `{r}`"),
                            });
                        }
                    }
                }
                for (k, x) in m {
                    let p = format!("{path}.{k}");
                    match props.and_then(|p| p.get(k)) {
                        Some(s) => self.check(base, s, x, &p, out),
                        None => out.push(Violation {
                            path: p,
                            problem: "field not declared by the lexicon".into(),
                        }),
                    }
                }
            }
            "array" => {
                let Some(a) = v.as_array() else {
                    return bad("expected array".into());
                };
                if let Some(max) = schema.get("maxLength").and_then(Value::as_u64) {
                    if a.len() as u64 > max {
                        bad(format!("array longer than {max}"));
                    }
                }
                if let Some(items) = schema.get("items") {
                    for (i, x) in a.iter().enumerate() {
                        self.check(base, items, x, &format!("{path}[{i}]"), out);
                    }
                }
            }
            "string" => {
                let Some(s) = v.as_str() else {
                    return bad("expected string".into());
                };
                if let Some(max) = schema.get("maxLength").and_then(Value::as_u64) {
                    if s.len() as u64 > max {
                        bad(format!("string longer than {max} bytes"));
                    }
                }
                match schema.get("format").and_then(Value::as_str) {
                    Some("did") if farsight_core::Did::parse(s).is_err() => {
                        bad(format!("{s:?} is not a DID"))
                    }
                    Some("at-uri") if farsight_core::AtUri::parse(s).is_err() => {
                        bad(format!("{s:?} is not an AT-URI"))
                    }
                    Some("datetime") if chrono::DateTime::parse_from_rfc3339(s).is_err() => {
                        bad(format!("{s:?} is not an RFC 3339 datetime"))
                    }
                    _ => {}
                }
            }
            "integer" => {
                let Some(n) = v.as_i64() else {
                    return bad("expected integer".into());
                };
                if let Some(min) = schema.get("minimum").and_then(Value::as_i64) {
                    if n < min {
                        bad(format!("{n} < minimum {min}"));
                    }
                }
                if let Some(max) = schema.get("maximum").and_then(Value::as_i64) {
                    if n > max {
                        bad(format!("{n} > maximum {max}"));
                    }
                }
            }
            "boolean" => {
                if !v.is_boolean() {
                    bad("expected boolean".into());
                }
            }
            // Fractional-second fields are declared `unknown` (lexicon has
            // no float type); their descriptions say they are numbers.
            "unknown" => {}
            other => bad(format!("unsupported schema type {other:?}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn files_load_and_refs_resolve() {
        let l = Lexicons::load();
        assert_eq!(l.docs.len(), FILES.len());
        assert_eq!(l.dangling_refs(), Vec::<String>::new());
        for e in crate::Endpoint::ALL {
            assert!(l.main(&e.nsid()).is_some(), "no lexicon for {}", e.nsid());
        }
    }

    #[test]
    fn validates_shapes() {
        let l = Lexicons::load();
        let fresh = json!({
            "asOf": "2026-10-01T00:00:00.000000Z",
            "firehoseConnected": true,
            "coverage": {
                "level": "partial", "reasons": ["sweep_incomplete"], "pendingLists": 0,
                "exceptions": {"unreachableRepos": 0, "pendingResyncs": 0, "cappedAuthors": 0,
                    "refusedAuthors": 0, "unavailableLists": 0, "missingLists": 0,
                    "deferredLists": 0, "cappedLists": 0, "excludedPendingLists": 0}
            }
        });
        let ok = json!({ "actor": "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa", "blocks": [
            {"did": "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb", "uri": "at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/app.bsky.graph.block/3l2x"}
        ], "freshness": fresh });
        assert_eq!(
            l.validate_output("app.nearhorizon.farsight.query.getIncomingBlocks", &ok),
            vec![]
        );
        let mut bad = ok.clone();
        bad["blocks"][0]["did"] = json!("alice.example");
        bad["extra"] = json!(1);
        let v = l.validate_output("app.nearhorizon.farsight.query.getIncomingBlocks", &bad);
        assert_eq!(v.len(), 2, "{v:?}");
        assert!(
            l.validate_error(
                "app.nearhorizon.farsight.admin.requestBackfill",
                &json!({"error": "QueueFull", "message": "x"})
            )
            .is_empty()
        );
    }
}
