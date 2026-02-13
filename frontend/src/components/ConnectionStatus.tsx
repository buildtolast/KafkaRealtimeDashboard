interface ConnectionStatusProps {
  connected: boolean;
}

export function ConnectionStatus({ connected }: ConnectionStatusProps) {
  return (
    <span className={`connection-status ${connected ? 'connected' : 'disconnected'}`}>
      <span className="status-dot" />
      {connected ? 'Live' : 'Reconnecting...'}
    </span>
  );
}
