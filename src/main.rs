use std::path::PathBuf;

use apollo_router::graphql;
use clap::Parser;
use tokio_stream::StreamExt;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::connect_async_tls_with_config;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use uuid::Uuid;
use websocket::convert_websocket_stream;
use websocket::GraphqlWebSocket;
use websocket::WebSocketProtocol;

mod websocket;

/// GraphQL websocket client to emulate several connections/subscriptions to a subgraph
#[derive(Parser)]
struct Cli {
    /// Number of websocket connection
    #[arg(short, long)]
    connections: usize,
    /// Url to the websocket url (example: ws://localhost:4002/graphql)
    #[arg(short, long)]
    url: String,
    /// Path to subscription file
    #[arg(short, long)]
    subscription_file: PathBuf,
    /// GraphQL websocket protocol
    #[arg(short, long)]
    protocol: WebSocketProtocol,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Cli::parse();

    let protocol = WebSocketProtocol::GraphqlWs;
    let query = std::fs::read_to_string(args.subscription_file).unwrap();
    for i in 1..=args.connections {
        client_connection(protocol, query.clone(), &args.url).await;
        if i % 100 == 0 {
            tracing::info!("{i} connection created");
        }
    }
    tokio::signal::ctrl_c().await.unwrap();
}

async fn client_connection(protocol: WebSocketProtocol, query: String, subgraph_url: &str) {
    let body = graphql::Request::builder().query(query).build();
    let request = get_websocket_request(subgraph_url, protocol);

    tracing::trace!("request = {request:?}");

    let (ws_stream, resp) = match request.uri().scheme_str() {
        Some("wss") => connect_async_tls_with_config(request, None, false, None).await,
        _ => connect_async(request).await,
    }
    .expect("cannot connect to websocket");

    let sub_id = Uuid::new_v4();
    let gql_socket = GraphqlWebSocket::new(
        convert_websocket_stream(ws_stream, sub_id.to_string()),
        sub_id.to_string(),
        protocol,
        None,
    )
    .await
    .unwrap();
    tracing::trace!("response = {resp:?}");

    let mut gql_stream = gql_socket.into_subscription(body, None).await.unwrap();

    tokio::task::spawn(async move {
        while let Some(data) = gql_stream.next().await {
            tracing::debug!("data = {data:?}");
        }
    });
}

fn get_websocket_request(subgraph_url: &str, ws_protocol: WebSocketProtocol) -> http::Request<()> {
    let mut request = subgraph_url
        .into_client_request()
        .map_err(|err| {
            tracing::error!("cannot create websocket client request: {err:?}");

            err
        })
        .unwrap();
    request
        .headers_mut()
        .insert(http::header::SEC_WEBSOCKET_PROTOCOL, ws_protocol.into());

    request
}
