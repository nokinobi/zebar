use std::time::Duration;

use anyhow::{bail, Context};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{net::TcpStream, sync::oneshot};
use tokio_tungstenite::{
  connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream,
};
use uuid::Uuid;

use crate::providers::{
  CommonProviderState, GlazeWmFunction, GlazeWmRunCommandArgs, Provider,
  ProviderFunction, ProviderFunctionResponse, ProviderFunctionResult,
  ProviderInputMsg, RuntimeType,
};

/// Address of GlazeWM's IPC server.
///
/// The connection is made from the backend rather than the widget
/// webview, since GlazeWM rejects websocket handshakes that carry an
/// `Origin` header (which browsers always attach).
const GLAZEWM_IPC_URL: &str = "ws://127.0.0.1:6123";

/// Delay before attempting to reconnect to GlazeWM.
const RECONNECT_INTERVAL: Duration = Duration::from_secs(5);

/// GlazeWM commands that widgets are not allowed to run.
///
/// `shell-exec` would let any widget run arbitrary programs and bypass
/// Zebar's own shell privilege checks.
const BLOCKED_COMMANDS: &[&str] = &["shell-exec"];

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GlazeWmProviderConfig {}

/// Raw GlazeWM state. Monitor-specific fields (e.g. the displayed
/// workspace) are derived by the client since they depend on the
/// widget's position.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GlazeWmOutput {
  pub all_monitors: Value,
  pub all_windows: Value,
  pub focused_container: Value,
  pub binding_modes: Value,
  pub tiling_direction: Value,
  pub is_paused: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GlazeWmRunCommandResponse {
  pub subject_container_id: String,
}

/// Message received from GlazeWM's IPC server.
#[derive(Deserialize, Debug)]
#[serde(tag = "messageType", rename_all = "snake_case")]
enum ServerMessage {
  ClientResponse(ClientResponseMessage),
  EventSubscription {},
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ClientResponseMessage {
  client_message: String,
  data: Option<Value>,
  error: Option<String>,
  success: bool,
}

pub struct GlazeWmProvider {
  common: CommonProviderState,
}

impl GlazeWmProvider {
  pub fn new(
    _config: GlazeWmProviderConfig,
    common: CommonProviderState,
  ) -> GlazeWmProvider {
    GlazeWmProvider { common }
  }

  /// Runs a single connection to GlazeWM until it closes or the
  /// provider is stopped.
  ///
  /// Returns `Ok` if the provider was stopped.
  async fn run_connection(&mut self) -> anyhow::Result<()> {
    let (mut ws, _) = connect_async(GLAZEWM_IPC_URL)
      .await
      .context("Failed to connect to GlazeWM IPC server.")?;

    // Whether an event was received while waiting for a response.
    let mut is_stale = false;

    request(&mut ws, "sub --events all", &mut is_stale).await?;
    self.emit_state(&mut ws, &mut is_stale).await?;

    loop {
      if is_stale {
        self.emit_state(&mut ws, &mut is_stale).await?;
      }

      tokio::select! {
        message = ws.next() => {
          match parse_message(message)? {
            Some(ServerMessage::EventSubscription {}) => is_stale = true,
            Some(ServerMessage::ClientResponse(_)) | None => {}
          }
        }
        Some(input) = self.common.input.async_rx.recv() => {
          match input {
            ProviderInputMsg::Stop => return Ok(()),
            ProviderInputMsg::Function(
              ProviderFunction::GlazeWm(function),
              sender,
            ) => {
              let res = handle_function(&mut ws, function, &mut is_stale)
                .await;
              send_function_result(sender, res);
            }
            ProviderInputMsg::Function(..) => {}
          }
        }
      }
    }
  }

  /// Queries the full GlazeWM state and emits it.
  async fn emit_state(
    &mut self,
    ws: &mut WsStream,
    is_stale: &mut bool,
  ) -> anyhow::Result<()> {
    *is_stale = false;

    let output = query_state(ws, is_stale).await;
    self.common.emitter.emit_output_cached(output);
    Ok(())
  }
}

#[async_trait]
impl Provider for GlazeWmProvider {
  fn runtime_type(&self) -> RuntimeType {
    RuntimeType::Async
  }

  async fn start_async(&mut self) {
    loop {
      let Err(err) = self.run_connection().await else {
        break;
      };

      tracing::debug!("GlazeWM connection ended: {err:?}");
      self.common.emitter.emit_output::<GlazeWmOutput>(Err(
        anyhow::anyhow!("Failed to connect to GlazeWM IPC server."),
      ));

      // Wait before reconnecting, while still handling inputs.
      let reconnect = tokio::time::sleep(RECONNECT_INTERVAL);
      tokio::pin!(reconnect);

      loop {
        tokio::select! {
          () = &mut reconnect => break,
          Some(input) = self.common.input.async_rx.recv() => {
            match input {
              ProviderInputMsg::Stop => return,
              ProviderInputMsg::Function(_, sender) => {
                send_function_result(
                  sender,
                  Err(anyhow::anyhow!("Not connected to GlazeWM.")),
                );
              }
            }
          }
        }
      }
    }
  }
}

async fn query_state(
  ws: &mut WsStream,
  is_stale: &mut bool,
) -> anyhow::Result<GlazeWmOutput> {
  let all_monitors =
    field(request(ws, "query monitors", is_stale).await?, "monitors")?;
  let all_windows =
    field(request(ws, "query windows", is_stale).await?, "windows")?;
  let focused_container =
    field(request(ws, "query focused", is_stale).await?, "focused")?;
  let binding_modes = field(
    request(ws, "query binding-modes", is_stale).await?,
    "bindingModes",
  )?;
  let tiling_direction = field(
    request(ws, "query tiling-direction", is_stale).await?,
    "tilingDirection",
  )?;

  // Paused state is only available on v3.7.0+ of GlazeWM.
  let is_paused = request(ws, "query paused", is_stale)
    .await
    .ok()
    .and_then(|paused| paused.as_bool())
    .unwrap_or(false);

  Ok(GlazeWmOutput {
    all_monitors,
    all_windows,
    focused_container,
    binding_modes,
    tiling_direction,
    is_paused,
  })
}

async fn handle_function(
  ws: &mut WsStream,
  function: GlazeWmFunction,
  is_stale: &mut bool,
) -> anyhow::Result<ProviderFunctionResponse> {
  match function {
    GlazeWmFunction::RunCommand(args) => {
      let message = build_command_message(&args)?;
      let data = request(ws, &message, is_stale).await?;

      Ok(ProviderFunctionResponse::GlazeWmRunCommand(
        GlazeWmRunCommandResponse {
          subject_container_id: field(data, "subjectContainerId")?
            .as_str()
            .context("Invalid command response from GlazeWM.")?
            .to_string(),
        },
      ))
    }
  }
}

/// Builds the IPC message for running a WM command.
///
/// Rejects blocked commands and messages that could inject extra flags
/// into the `command` invocation.
fn build_command_message(
  args: &GlazeWmRunCommandArgs,
) -> anyhow::Result<String> {
  // GlazeWM splits incoming messages on whitespace.
  let tokens = args.command.split_whitespace().collect::<Vec<_>>();

  let Some(first_token) = tokens.first() else {
    bail!("Command cannot be empty.");
  };

  if first_token.starts_with('-') {
    bail!("Command must start with a command name.");
  }

  if let Some(blocked) = tokens.iter().find(|token| {
    BLOCKED_COMMANDS
      .iter()
      .any(|blocked| token.eq_ignore_ascii_case(blocked))
  }) {
    bail!("GlazeWM command '{blocked}' is not allowed from widgets.");
  }

  let command = tokens.join(" ");

  match &args.subject_container_id {
    Some(id) => {
      let id = Uuid::parse_str(id).context("Invalid container ID.")?;
      Ok(format!("command --id {id} {command}"))
    }
    None => Ok(format!("command {command}")),
  }
}

/// Sends a message to GlazeWM and waits for its response.
///
/// Sets `is_stale` if any events arrive in the meantime.
async fn request(
  ws: &mut WsStream,
  message: &str,
  is_stale: &mut bool,
) -> anyhow::Result<Value> {
  ws.send(Message::text(message)).await?;

  loop {
    match parse_message(ws.next().await)? {
      Some(ServerMessage::ClientResponse(res))
        if res.client_message == message =>
      {
        if !res.success {
          bail!(res.error.unwrap_or_else(|| "Unknown error.".into()));
        }

        return Ok(res.data.unwrap_or(Value::Null));
      }
      Some(ServerMessage::EventSubscription {}) => *is_stale = true,
      _ => {}
    }
  }
}

/// Parses a websocket message from GlazeWM.
///
/// Returns `None` for non-text frames and errors if the connection is
/// closed.
fn parse_message(
  message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
) -> anyhow::Result<Option<ServerMessage>> {
  match message.context("GlazeWM IPC connection closed.")?? {
    Message::Text(text) => Ok(serde_json::from_str(&text).ok()),
    Message::Close(_) => bail!("GlazeWM IPC connection closed."),
    _ => Ok(None),
  }
}

/// Extracts a field from a response's data.
fn field(mut data: Value, key: &str) -> anyhow::Result<Value> {
  data
    .get_mut(key)
    .map(Value::take)
    .with_context(|| format!("Missing '{key}' in GlazeWM response."))
}

fn send_function_result(
  sender: oneshot::Sender<ProviderFunctionResult>,
  res: anyhow::Result<ProviderFunctionResponse>,
) {
  let _ = sender.send(res.map_err(|err| err.to_string()));
}

#[cfg(test)]
mod tests {
  use super::*;

  fn args(command: &str, id: Option<&str>) -> GlazeWmRunCommandArgs {
    GlazeWmRunCommandArgs {
      command: command.into(),
      subject_container_id: id.map(Into::into),
    }
  }

  #[test]
  fn builds_command_message() {
    assert_eq!(
      build_command_message(&args("focus  --workspace 1", None)).unwrap(),
      "command focus --workspace 1"
    );

    let id = "6f9619ff-8b86-d011-b42d-00c04fc964ff";
    assert_eq!(
      build_command_message(&args("close", Some(id))).unwrap(),
      format!("command --id {id} close")
    );
  }

  #[test]
  fn rejects_blocked_and_injected_commands() {
    for command in [
      "shell-exec calc",
      "SHELL-EXEC calc",
      "--id 6f9619ff-8b86-d011-b42d-00c04fc964ff shell-exec calc",
      "--hide-window close",
      "",
      "   ",
    ] {
      assert!(build_command_message(&args(command, None)).is_err());
    }

    assert!(
      build_command_message(&args("close", Some("x shell-exec calc")))
        .is_err()
    );
  }
}
