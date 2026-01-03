use thiserror::Error;

#[derive(Error, Debug)]
pub enum WebsocketError {
    #[error("Failed to parse the incoming message")]
    JsonParse(#[from] serde_json::Error),
    #[error("The worker is not authenticated")]
    NotAuthenticated,
    #[error("An invalid secret was provided by the incoming worker")]
    AuthenticationFailure,
    #[error("Received an unknown message type")]
    UnknownMessage,
    #[error("The worker has disconnected")]
    ClientDisconnect,
}