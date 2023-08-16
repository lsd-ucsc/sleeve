use std::env;

use axum::{extract::State, routing, Json, Router};
use serde_json::Value;

#[derive(serde::Deserialize, serde::Serialize, Debug)]
struct EventList {
    metadata: Value,
    items: Vec<Value>,
}

async fn handle(State(db_client): State<mongodb::Client>, Json(events): Json<EventList>) {
    dbg!("Received events: {:?}", events.metadata);
    db_client
        .database("kartographer")
        .collection("events")
        .insert_many(events.items, None)
        .await
        .unwrap();
}

#[tokio::main]
async fn main() {
    let db_uri = env::var("MONGODB_URI").expect("MONGODB_URI not set");
    let db_options = mongodb::options::ClientOptions::parse(&db_uri)
        .await
        .expect("Failed to parse MongoDB options");
    let db_client =
        mongodb::Client::with_options(db_options).expect("Failed to create MongoDB client");

    let app = Router::new()
        .route("/", routing::post(handle))
        .with_state(db_client);

    dbg!("Listening on [::]:8080");
    axum::Server::bind(&"[::]:8080".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
