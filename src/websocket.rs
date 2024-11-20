use std::borrow::Cow;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use apollo_router::graphql;
use futures::future;
use futures::stream::SplitStream;
use futures::Future;
use futures::Sink;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use http::HeaderValue;
use pin_project_lite::pin_project;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use serde_json_bytes::Value;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio_stream::wrappers::IntervalStream;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const CONNECTION_ACK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema, Copy, clap::ValueEnum,
)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WebSocketProtocol {
    // New one
    #[value(name = "graphql_ws")]
    GraphqlWs,
    // Old one
    #[serde(rename = "graphql_transport_ws")]
    #[value(name = "graphql_transport_ws")]
    SubscriptionsTransportWs,
}

impl Default for WebSocketProtocol {
    fn default() -> Self {
        Self::GraphqlWs
    }
}

impl From<WebSocketProtocol> for HeaderValue {
    fn from(value: WebSocketProtocol) -> Self {
        match value {
            WebSocketProtocol::GraphqlWs => HeaderValue::from_static("graphql-transport-ws"),
            WebSocketProtocol::SubscriptionsTransportWs => HeaderValue::from_static("graphql-ws"),
        }
    }
}

impl WebSocketProtocol {
    fn subscribe(&self, id: String, payload: graphql::Request) -> ClientMessage {
        match self {
            // old
            WebSocketProtocol::SubscriptionsTransportWs => ClientMessage::OldStart { id, payload },
            // new
            WebSocketProtocol::GraphqlWs => ClientMessage::Subscribe { id, payload },
        }
    }

    fn complete(&self, id: String) -> ClientMessage {
        match self {
            // old
            WebSocketProtocol::SubscriptionsTransportWs => ClientMessage::OldStop { id },
            // new
            WebSocketProtocol::GraphqlWs => ClientMessage::Complete { id },
        }
    }
}

/// A websocket message received from the client
#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)] // Request is at fault
pub(crate) enum ClientMessage {
    /// A new connection
    ConnectionInit {
        /// Optional init payload from the client
        payload: Option<serde_json_bytes::Value>,
    },
    /// The start of a Websocket subscription
    Subscribe {
        /// Message ID
        id: String,
        /// The GraphQL Request - this can be modified by protocol implementors
        /// to add files uploads.
        payload: graphql::Request,
    },
    #[serde(rename = "start")]
    /// For old protocol
    OldStart {
        /// Message ID
        id: String,
        /// The GraphQL Request - this can be modified by protocol implementors
        /// to add files uploads.
        payload: graphql::Request,
    },
    /// The end of a Websocket subscription
    Complete {
        /// Message ID
        id: String,
    },
    /// For old protocol
    #[serde(rename = "stop")]
    OldStop {
        /// Message ID
        id: String,
    },
    /// Connection terminated by the client
    ConnectionTerminate,
    /// Close the websocket connection (not related to any graphql sub protocol)
    CloseWebsocket,
    /// Useful for detecting failed connections, displaying latency metrics or
    /// other types of network probing.
    ///
    /// Reference: <https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md#ping>
    Ping {
        /// Additional details about the ping.
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<serde_json_bytes::Value>,
    },
    /// The response to the Ping message.
    ///
    /// Reference: <https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md#pong>
    Pong {
        /// Additional details about the pong.
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<serde_json_bytes::Value>,
    },
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ServerMessage {
    ConnectionAck,
    /// subscriptions-transport-ws protocol alias for next payload
    #[serde(alias = "data")]
    /// graphql-ws protocol next payload
    Next {
        id: String,
        payload: graphql::Response,
    },
    #[serde(alias = "connection_error")]
    Error {
        id: String,
        payload: ServerError,
    },
    Complete {
        id: String,
    },
    #[serde(alias = "ka")]
    KeepAlive,
    /// The response to the Ping message.
    ///
    /// https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md#pong
    Pong {
        payload: Option<serde_json::Value>,
    },
    Ping {
        payload: Option<serde_json::Value>,
    },
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
pub(crate) enum ServerError {
    Error(graphql::Error),
    Errors(Vec<graphql::Error>),
}

impl From<ServerError> for Vec<graphql::Error> {
    fn from(value: ServerError) -> Self {
        match value {
            ServerError::Error(e) => vec![e],
            ServerError::Errors(e) => e,
        }
    }
}

impl ServerMessage {
    fn into_graphql_response(self) -> (Option<graphql::Response>, bool) {
        match self {
            ServerMessage::Next { id: _, mut payload } => {
                payload.subscribed = Some(true);
                (Some(payload), false)
            }
            ServerMessage::Error { id: _, payload } => (
                Some(
                    graphql::Response::builder()
                        .errors(payload.into())
                        .subscribed(false)
                        .build(),
                ),
                true,
            ),
            ServerMessage::Complete { .. } => (None, true),
            ServerMessage::ConnectionAck | ServerMessage::Pong { .. } => (None, false),
            ServerMessage::Ping { .. } => (None, false),
            ServerMessage::KeepAlive => (None, false),
        }
    }

    fn id(&self) -> Option<String> {
        match self {
            ServerMessage::ConnectionAck
            | ServerMessage::KeepAlive
            | ServerMessage::Ping { .. }
            | ServerMessage::Pong { .. } => None,
            ServerMessage::Next { id, .. }
            | ServerMessage::Error { id, .. }
            | ServerMessage::Complete { id } => Some(id.to_string()),
        }
    }
}

pub(crate) struct GraphqlWebSocket<S> {
    stream: S,
    id: String,
    protocol: WebSocketProtocol,
}

impl<S> GraphqlWebSocket<S>
where
    S: Stream<Item = serde_json::Result<ServerMessage>>
        + Sink<ClientMessage>
        + std::marker::Unpin
        + std::marker::Send
        + 'static,
{
    pub(crate) async fn new(
        mut stream: S,
        id: String,
        protocol: WebSocketProtocol,
        connection_params: Option<Value>,
    ) -> Result<Self, graphql::Error> {
        let connection_init_msg = match connection_params {
            Some(connection_params) => ClientMessage::ConnectionInit {
                payload: Some(serde_json_bytes::json!({
                    "connectionParams": connection_params
                })),
            },
            None => ClientMessage::ConnectionInit { payload: None },
        };
        stream.send(connection_init_msg).await.map_err(|_err| {
            graphql::Error::builder()
                .message("cannot send connection init through websocket connection")
                .extension_code("WEBSOCKET_INIT_ERROR")
                .build()
        })?;

        let first_non_ping_payload = async {
            loop {
                match stream.next().await {
                    Some(Ok(ServerMessage::Ping { payload })) => {
                        // we don't mind an error here
                        // because it will fall through the error below
                        // if we haven't been able to properly get a ConnectionAck within the `CONNECTION_ACK_TIMEOUT`
                        let _ = stream
                            .send(ClientMessage::Pong {
                                payload: payload.map(|p| p.into()),
                            })
                            .await;
                    }
                    other => {
                        return other;
                    }
                }
            }
        };

        let resp = tokio::time::timeout(CONNECTION_ACK_TIMEOUT, first_non_ping_payload)
            .await
            .map_err(|_| {
                graphql::Error::builder()
                    .message("cannot receive connection ack from websocket connection")
                    .extension_code("WEBSOCKET_ACK_ERROR_TIMEOUT")
                    .build()
            })?;
        if !matches!(resp, Some(Ok(ServerMessage::ConnectionAck))) {
            return Err(graphql::Error::builder()
                .message(format!("didn't receive the connection ack from websocket connection but instead got: {:?}", resp))
                .extension_code("WEBSOCKET_ACK_ERROR")
                .build());
        }

        Ok(Self {
            stream,
            id,
            protocol,
        })
    }

    pub(crate) async fn into_subscription(
        mut self,
        request: graphql::Request,
        heartbeat_interval: Option<tokio::time::Duration>,
    ) -> Result<SubscriptionStream<S>, graphql::Error> {
        self.stream
            .send(self.protocol.subscribe(self.id.to_string(), request))
            .await
            .map(|_| {
                SubscriptionStream::new(self.stream, self.id, self.protocol, heartbeat_interval)
            })
            .map_err(|_err| {
                graphql::Error::builder()
                    .message("cannot send to websocket connection")
                    .extension_code("WEBSOCKET_CONNECTION_ERROR")
                    .build()
            })
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    #[error("websocket error")]
    WebSocketError(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("deserialization/serialization error")]
    SerdeError(#[from] serde_json::Error),
}

pub(crate) fn convert_websocket_stream<T>(
    stream: WebSocketStream<T>,
    id: String,
) -> impl Stream<Item = serde_json::Result<ServerMessage>> + Sink<ClientMessage, Error = Error>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    stream
        .with(|client_message: ClientMessage| {
            // It applies to the Sink
            match client_message {
                ClientMessage::CloseWebsocket => {
                    future::ready(Ok(Message::Close(Some(CloseFrame{
                        code: CloseCode::Normal,
                        reason: Cow::default(),
                    }))))
                },
                message => {
                    future::ready(match serde_json::to_string(&message) {
                        Ok(client_message_str) => Ok(Message::Text(client_message_str)),
                        Err(err) => Err(Error::SerdeError(err)),
                    })
                },
            }
        })
        .map(move |msg| match msg {
            // It applies to the Stream
            Ok(Message::Text(text)) => serde_json::from_str(&text),
            Ok(Message::Binary(bin)) => serde_json::from_slice(&bin),
            Ok(Message::Ping(payload)) => Ok(ServerMessage::Ping {
                payload: serde_json::from_slice(&payload).ok(),
            }),
            Ok(Message::Pong(payload)) => Ok(ServerMessage::Pong {
                payload: serde_json::from_slice(&payload).ok(),
            }),
            Ok(Message::Close(None)) => Ok(ServerMessage::Complete { id: id.to_string() }),
            Ok(Message::Close(Some(CloseFrame{ code, reason }))) => {
                if code == CloseCode::Normal {
                    Ok(ServerMessage::Complete { id: id.to_string() })
                } else {
                    Ok(ServerMessage::Error {
                        id: id.to_string(),
                        payload: ServerError::Error(
                            graphql::Error::builder()
                                .message(format!("websocket connection has been closed with error code '{code}' and reason '{reason}'"))
                                .extension_code("WEBSOCKET_CLOSE_ERROR")
                                .build(),
                        ),
                    })
                }
            }
            Ok(Message::Frame(frame)) => serde_json::from_slice(frame.payload()),
            Err(err) => {
                tracing::error!("cannot consume more message on websocket stream: {err:?}");

                Ok(ServerMessage::Error {
                    id: id.to_string(),
                    payload: ServerError::Error(
                        graphql::Error::builder()
                            .message("cannot read message from websocket")
                            .extension_code("WEBSOCKET_MESSAGE_ERROR")
                            .build(),
                    ),
                })
            }
        })
}

pub(crate) struct SubscriptionStream<S> {
    inner_stream: SplitStream<InnerStream<S>>,
    close_signal: Option<tokio::sync::oneshot::Sender<()>>,
}

impl<S> SubscriptionStream<S>
where
    S: Stream<Item = serde_json::Result<ServerMessage>>
        + Sink<ClientMessage>
        + std::marker::Unpin
        + std::marker::Send
        + 'static,
{
    pub(crate) fn new(
        stream: S,
        id: String,
        protocol: WebSocketProtocol,
        heartbeat_interval: Option<tokio::time::Duration>,
    ) -> Self {
        let (mut sink, inner_stream) = InnerStream::new(stream, id, protocol).split();
        let (close_signal, close_sentinel) = tokio::sync::oneshot::channel::<()>();

        tokio::task::spawn(async move {
            if let (WebSocketProtocol::GraphqlWs, Some(duration)) = (protocol, heartbeat_interval) {
                let mut interval =
                    tokio::time::interval_at(tokio::time::Instant::now() + duration, duration);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut heartbeat_stream = IntervalStream::new(interval)
                    .map(|_| Ok(ClientMessage::Ping { payload: None }))
                    .take_until(close_sentinel);
                if let Err(err) = sink.send_all(&mut heartbeat_stream).await {
                    tracing::trace!("cannot send heartbeat: {err:?}");
                    if let Some(close_sentinel) = heartbeat_stream.take_future() {
                        if let Err(err) = close_sentinel.await {
                            tracing::trace!("cannot shutdown sink: {err:?}");
                        }
                    }
                }
            } else if let Err(err) = close_sentinel.await {
                tracing::trace!("cannot shutdown sink: {err:?}");
            };

            if let Err(err) = sink.close().await {
                tracing::trace!("cannot close the websocket stream: {err:?}");
            }
        });

        Self {
            inner_stream,
            close_signal: Some(close_signal),
        }
    }
}

impl<S> Drop for SubscriptionStream<S> {
    fn drop(&mut self) {
        if let Some(close_signal) = self.close_signal.take() {
            if let Err(err) = close_signal.send(()) {
                tracing::trace!("cannot close the websocket stream: {err:?}");
            }
        }
    }
}

impl<S> Stream for SubscriptionStream<S>
where
    S: Stream<Item = serde_json::Result<ServerMessage>> + Sink<ClientMessage> + std::marker::Unpin,
{
    type Item = graphql::Response;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner_stream.poll_next_unpin(cx)
    }
}

pin_project! {
struct InnerStream<S> {
    #[pin]
    stream: S,
    id: String,
    protocol: WebSocketProtocol,
    // Booleans for state machine when closing the stream
    completed: bool,
    terminated: bool,
    // When the websocket stream is closed (!= graphql sub protocol)
    closed: bool,
}
}

impl<S> InnerStream<S>
where
    S: Stream<Item = serde_json::Result<ServerMessage>> + Sink<ClientMessage> + std::marker::Unpin,
{
    fn new(stream: S, id: String, protocol: WebSocketProtocol) -> Self {
        Self {
            stream,
            id,
            protocol,
            completed: false,
            terminated: false,
            closed: false,
        }
    }
}

impl<S> Stream for InnerStream<S>
where
    S: Stream<Item = serde_json::Result<ServerMessage>> + Sink<ClientMessage>,
{
    type Item = graphql::Response;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(message) => match message {
                Some(server_message) => match server_message {
                    Ok(server_message) => {
                        if let Some(id) = &server_message.id() {
                            if this.id != id {
                                tracing::error!("we should not receive data from other subscriptions, closing the stream");
                                return Poll::Ready(None);
                            }
                        }
                        if let ServerMessage::Ping { .. } = server_message {
                            // Send pong asynchronously
                            let _ = Pin::new(
                                &mut Pin::new(&mut this.stream)
                                    .send(ClientMessage::Pong { payload: None }),
                            )
                            .poll(cx);
                        }
                        match server_message.into_graphql_response() {
                            (None, true) => Poll::Ready(None),
                            // For ignored message like ACK, Ping, Pong, etc...
                            (None, false) => self.poll_next(cx),
                            (Some(resp), _) => Poll::Ready(Some(resp)),
                        }
                    }
                    Err(err) => Poll::Ready(
                        graphql::Response::builder()
                            .error(
                                graphql::Error::builder()
                                    .message(format!(
                                        "cannot deserialize websocket server message: {err:?}"
                                    ))
                                    .extension_code("INVALID_WEBSOCKET_SERVER_MESSAGE_FORMAT")
                                    .build(),
                            )
                            .build()
                            .into(),
                    ),
                },
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> Sink<ClientMessage> for InnerStream<S>
where
    S: Stream<Item = serde_json::Result<ServerMessage>> + Sink<ClientMessage>,
{
    type Error = graphql::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();

        match Pin::new(&mut this.stream).poll_ready(cx) {
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(_err)) => Poll::Ready(Err("websocket connection error")),
            Poll::Pending => Poll::Pending,
        }
        .map_err(|err| {
            graphql::Error::builder()
                .message(format!("cannot establish websocket connection: {err}"))
                .extension_code("WEBSOCKET_CONNECTION_ERROR")
                .build()
        })
    }

    fn start_send(self: Pin<&mut Self>, item: ClientMessage) -> Result<(), Self::Error> {
        let mut this = self.project();

        Pin::new(&mut this.stream).start_send(item).map_err(|_err| {
            graphql::Error::builder()
                .message("cannot send to websocket connection")
                .extension_code("WEBSOCKET_CONNECTION_ERROR")
                .build()
        })
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        Pin::new(&mut this.stream).poll_flush(cx).map_err(|_err| {
            graphql::Error::builder()
                .message("cannot flush to websocket connection")
                .extension_code("WEBSOCKET_CONNECTION_ERROR")
                .build()
        })
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        if !*this.completed {
            match Pin::new(
                &mut Pin::new(&mut this.stream).send(this.protocol.complete(this.id.to_string())),
            )
            .poll(cx)
            {
                Poll::Ready(_) => {
                    *this.completed = true;
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
        if let WebSocketProtocol::SubscriptionsTransportWs = this.protocol {
            if !*this.terminated {
                match Pin::new(
                    &mut Pin::new(&mut this.stream).send(ClientMessage::ConnectionTerminate),
                )
                .poll(cx)
                {
                    Poll::Ready(_) => {
                        *this.terminated = true;
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                }
            }
        }

        if !*this.closed {
            // instead of just calling poll_close we also send a proper CloseWebsocket event to indicate it's a normal close, not an error
            match Pin::new(&mut Pin::new(&mut this.stream).send(ClientMessage::CloseWebsocket))
                .poll(cx)
            {
                Poll::Ready(_) => {
                    *this.closed = true;
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        Pin::new(&mut this.stream).poll_close(cx).map_err(|_err| {
            graphql::Error::builder()
                .message("cannot close websocket connection")
                .extension_code("WEBSOCKET_CONNECTION_ERROR")
                .build()
        })
    }
}
