//! Consumes OrderCreated from the `orders` topic exchange and marks the row processed.
//!
//! The queue binds with the wildcard pattern `*.process`, so which producer routing
//! keys actually land here cannot be decided from this file alone.

use anyhow::Result;
use futures_lite::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    Connection, ConnectionProperties, ExchangeKind,
};
use serde::Deserialize;
use sqlx::PgPool;

const EXCHANGE: &str = "orders";
const QUEUE: &str = "orders.process";
const BINDING_KEY: &str = "*.process";
const CONSUMER_TAG: &str = "wnwfx-worker";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderCreated {
    order_id: String,
}

pub async fn run(amqp_url: &str, pool: PgPool) -> Result<()> {
    let connection = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;

    channel
        .exchange_declare(
            EXCHANGE,
            ExchangeKind::Topic,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_declare(
            QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_bind(
            QUEUE,
            EXCHANGE,
            BINDING_KEY,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut consumer = channel
        .basic_consume(
            QUEUE,
            CONSUMER_TAG,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    tracing::info!(queue = QUEUE, binding = BINDING_KEY, "consuming");

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;
        match serde_json::from_slice::<OrderCreated>(&delivery.data) {
            Ok(event) => {
                if let Err(error) = mark_processed(&pool, &event.order_id).await {
                    tracing::error!(?error, order_id = event.order_id, "update failed");
                }
            }
            Err(error) => tracing::error!(?error, "undecodable delivery"),
        }
        delivery.ack(BasicAckOptions::default()).await?;
    }

    Ok(())
}

/// Writes the shared `orders` table — wnwfx-checkout owns the same table.
async fn mark_processed(pool: &PgPool, order_id: &str) -> Result<()> {
    sqlx::query("UPDATE orders SET status='processed' WHERE id = $1")
        .bind(order_id)
        .execute(pool)
        .await?;
    Ok(())
}
