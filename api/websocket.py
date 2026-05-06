from fastapi import WebSocket

from metrics import ws_connections_active


class ConnectionManager:
    """Tracks active WebSocket connections and fans out messages to all of them."""

    def __init__(self) -> None:
        self._connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._connections.append(websocket)
        ws_connections_active.inc()

    def disconnect(self, websocket: WebSocket) -> None:
        self._connections.remove(websocket)
        ws_connections_active.dec()

    async def broadcast(self, message: dict) -> None:
        dead: list[WebSocket] = []
        for ws in self._connections:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._connections.remove(ws)
            ws_connections_active.dec()


manager = ConnectionManager()
