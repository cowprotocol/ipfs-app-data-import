use anyhow::{Context, Result};
use tokio_postgres::{types::Type, Client, Statement};

use crate::AppDataHash;

pub struct Postgres {
    client: Client,
    insert: Statement,
}

impl Postgres {
    pub async fn new(url: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .context("connect")?;
        tokio::spawn(async move {
            connection.await.context("connection closed").unwrap();
        });
        let insert = r#"
INSERT INTO app_data (contract_app_data, full_app_data)
VALUES ($1, $2)
ON CONFLICT DO NOTHING
;"#;
        let insert = client
            .prepare_typed(insert, &[Type::BYTEA, Type::BYTEA])
            .await
            .context("prepare_typed")?;
        Ok(Self { client, insert })
    }

    pub async fn app_data_without_full(&self) -> Result<Vec<AppDataHash>> {
        let query = r#"
SELECT DISTINCT(app_data)
FROM orders
LEFT OUTER JOIN app_data ON (app_data = contract_app_data)
WHERE full_app_data IS NULL
;"#;
        let rows = self.client.query(query, &[]).await.context("query")?;
        rows.into_iter()
            .map(|row| {
                let app_data_hash: &[u8] = row.try_get(0).context("try_get")?;
                AppDataHash::try_from(app_data_hash).context("try_from")
            })
            .collect()
    }

    pub async fn insert(&self, app_data_hash: &AppDataHash, full: &[u8]) -> Result<()> {
        self.client
            .execute(&self.insert, &[&app_data_hash.as_slice(), &full])
            .await
            .context("execute")
            .map(|_| ())
    }
}
