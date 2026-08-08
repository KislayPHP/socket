// Hand-rolled Engine.IO/Socket.IO v2-style client over a raw WebSocket.
// No official JS client exists anywhere in this repo (Kislay\Socket\Server
// is Engine.IO/Socket.IO-*wire-compatible*, not a bundled product), and the
// server applies the exact same text-packet parser to WebSocket frames as it
// does to polling requests - so even over a native WebSocket, messages must
// still be framed as:
//   server -> client "0" + json   (open packet, carries the session id)
//   client -> server "40"         (CONNECT - required before events work)
//   server -> client "2"          (ping)
//   client -> server "3"          (pong reply)
//   either direction "42" + json  (EVENT: json is ["eventName", data])

(function () {
    const statusEl = document.getElementById('status');
    const roomForm = document.getElementById('room-form');
    const roomInput = document.getElementById('room-input');
    const countEl = document.getElementById('user-count');
    const messagesEl = document.getElementById('messages');
    const chatForm = document.getElementById('chat-form');
    const chatInput = document.getElementById('chat-input');

    let ws = null;
    let currentRoom = null;

    function setStatus(text, cls) {
        statusEl.textContent = text;
        statusEl.className = cls;
    }

    function addMessage(text, cls) {
        const li = document.createElement('li');
        li.textContent = text;
        if (cls) li.className = cls;
        messagesEl.appendChild(li);
        messagesEl.scrollTop = messagesEl.scrollHeight;
    }

    function emit(event, data) {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send('42' + JSON.stringify([event, data]));
        }
    }

    function connect() {
        const proto = location.protocol === 'https:' ? 'wss' : 'ws';
        const host = window.CHAT_SOCKET_HOST || location.hostname;
        const port = window.CHAT_SOCKET_PORT || 9200;
        ws = new WebSocket(`${proto}://${host}:${port}/socket.io/?EIO=4&transport=websocket`);

        setStatus('connecting…', 'status-pending');

        ws.onopen = () => {
            // Wait for the server's "0{...}" open packet before doing anything.
        };

        ws.onclose = () => {
            setStatus('disconnected', 'status-down');
            chatForm.querySelector('button').disabled = true;
        };

        ws.onerror = () => {
            setStatus('connection error', 'status-down');
        };

        ws.onmessage = (event) => {
            const msg = event.data;
            if (msg === '2') {
                ws.send('3');
                return;
            }
            if (msg[0] === '0') {
                ws.send('40');
                return;
            }
            if (msg.startsWith('40')) {
                setStatus('connected', 'status-up');
                if (currentRoom) {
                    emit('join_room', { room: currentRoom });
                }
                return;
            }
            if (msg.startsWith('41')) {
                setStatus('rejected', 'status-down');
                return;
            }
            if (msg.startsWith('42')) {
                let name, data;
                try {
                    [name, data] = JSON.parse(msg.slice(2));
                } catch (e) {
                    return;
                }
                handleEvent(name, data);
                return;
            }
        };
    }

    function handleEvent(name, data) {
        if (name === 'joined') {
            countEl.textContent = data.count;
            chatForm.querySelector('button').disabled = false;
            addMessage(`— joined "${data.room}" (${data.count} online) —`, 'system');
        } else if (name === 'user_joined') {
            countEl.textContent = data.count;
            addMessage(`— someone joined (${data.count} online) —`, 'system');
        } else if (name === 'chat_message') {
            const when = new Date(data.ts * 1000).toLocaleTimeString();
            addMessage(`[${when}] ${data.from.slice(0, 8)}: ${data.text}`);
        }
    }

    roomForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const room = roomInput.value.trim() || 'lobby';
        currentRoom = room;
        messagesEl.innerHTML = '';
        emit('join_room', { room });
    });

    chatForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const text = chatInput.value.trim();
        if (!text || !currentRoom) return;
        emit('chat_message', { room: currentRoom, text });
        chatInput.value = '';
    });

    connect();
})();
