use foundry_connector::FoundryCredential;
use serde_json::{json, Value};
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct Instances {
    credentials: Arc<Vec<FoundryCredential>>,
    selected: Arc<RwLock<usize>>,
}

impl Instances {
    pub fn new(credentials: Vec<FoundryCredential>) -> anyhow::Result<Self> {
        if credentials.is_empty() {
            anyhow::bail!("credentials file contains no entries");
        }
        Ok(Self {
            credentials: Arc::new(credentials),
            selected: Arc::new(RwLock::new(0)),
        })
    }

    pub fn snapshot(&self) -> FoundryCredential {
        self.credentials[*self.selected.read().unwrap()].clone()
    }

    pub fn show(&self) -> Value {
        let selected = *self.selected.read().unwrap();
        Value::Array(
            self.credentials
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    json!({
                        "_id": c.id, "hostname": c.hostname, "userid": c.userid,
                        "item_order": i, "currently_active": i == selected
                    })
                })
                .collect(),
        )
    }

    pub fn choose(&self, index: usize) -> anyhow::Result<()> {
        if index >= self.credentials.len() {
            anyhow::bail!("instance not found");
        }
        *self.selected.write().unwrap() = index;
        Ok(())
    }

    pub fn select(&self, args: &Value) -> anyhow::Result<Value> {
        let id = args.get("_id").or_else(|| args.get("id"));
        let order = args.get("item_order");
        if id.is_some() == order.is_some() {
            anyhow::bail!("provide exactly one of _id or item_order");
        }
        let index = if let Some(id) = id {
            let id = id
                .as_str()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow::anyhow!("invalid instance identifier"))?;
            if args.get("_id").is_some() && args.get("id").is_some() {
                anyhow::bail!("provide only one identifier");
            }
            let mut matches = self
                .credentials
                .iter()
                .enumerate()
                .filter(|(_, c)| c.id.as_deref() == Some(id));
            let index = matches
                .next()
                .map(|(i, _)| i)
                .ok_or_else(|| anyhow::anyhow!("instance not found"))?;
            if matches.next().is_some() {
                anyhow::bail!("ambiguous instance identifier");
            }
            index
        } else {
            order
                .and_then(Value::as_u64)
                .and_then(|n| usize::try_from(n).ok())
                .ok_or_else(|| anyhow::anyhow!("invalid item_order"))?
        };
        self.choose(index)?;
        Ok(json!({"item_order": index, "selected": true}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_preserves_snapshots_and_never_discloses_secrets() {
        let make = |id: &str, password: &str| FoundryCredential {
            id: Some(id.into()),
            hostname: "localhost".into(),
            userid: None,
            username: None,
            password: password.into(),
        };
        let instances = Instances::new(vec![
            make("first", "secret-one"),
            make("second", "secret-two"),
        ])
        .unwrap();
        let snapshot = instances.snapshot();
        assert_eq!(snapshot.id.as_deref(), Some("first"));
        instances.choose(1).unwrap();
        assert_eq!(snapshot.id.as_deref(), Some("first"));
        assert_eq!(instances.snapshot().id.as_deref(), Some("second"));
        assert!(instances.choose(2).is_err());
        assert_eq!(instances.snapshot().id.as_deref(), Some("second"));
        let output = instances.show().to_string();
        assert!(!output.contains("secret-one"));
        assert!(!output.contains("secret-two"));
    }
}
