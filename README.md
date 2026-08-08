# wnwfx-worker

Rust 2021 order-processing worker for the wnwfx fixture system.

- lapin consumer on queue `orders.process`, bound to the `orders` topic exchange with `*.process`, consumer tag `wnwfx-worker`.
- sqlx marks rows processed in the shared `orders` table via `DATABASE_URL`.
- axum `GET /health` on `:8082`.

Broker address comes from `AMQP_URL`; both are bound in wnwfx-infra.
