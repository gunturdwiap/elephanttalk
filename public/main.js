// @ts-check
"use strict";

/**
 * @typedef {"chat" | "typing" } ActionType
 */

/**
 * @template T
 * @typedef {Object} Action
 * @property {ActionType} type
 * @property {T} payload
 */

/**
 * @typedef {Object} ChatPayload
 * @property {string} sender
 * @property {string} sent_at
 * @property {string} message
 */

/**
 * @typedef {Object} TypingPayload
 * @property {string} sender
 * @property {boolean} is_typing
 */

/**
 * @param {string} sender
 * @param {string} timestamp
 * @param {string} msg
 * @returns {string}
 */
function renderChatBox(sender, timestamp, msg) {
  const timeStr = new Date(timestamp).toLocaleTimeString();
  return `
    <article>
      <header>
        <small><strong>${sender}</strong> at ${timeStr}</small>
      </header>
      <p>${msg}</p>
    </article>
  `;
}

/**
 * @param {WebSocket} conn
 * @param {string} type
 * @param {object} payload
 */
function sendAction(conn, type, payload) {
  if (conn.readyState === WebSocket.OPEN) {
    conn.send(JSON.stringify({ type, payload }));
  }
}

/**
 * @param {MessageEvent} event
 * @param {HTMLElement} chatSection
 * @param {HTMLElement} typingIndicator
 */
function handleIncomingMessage(event, chatSection, typingIndicator) {
  try {
    /** @type {Action<any>} */
    const res = JSON.parse(event.data);

    switch (res.type) {
      case "chat": {
        /** @type {ChatPayload} */
        const p = res.payload;
        const html = renderChatBox(p.sender, p.sent_at, p.message);
        chatSection.insertAdjacentHTML("beforeend", html);
        chatSection.scrollTop = chatSection.scrollHeight;
        break;
      }
      case "typing": {
        /** @type {TypingPayload} */
        const p = res.payload;
        if (p.is_typing) {
          typingIndicator.textContent = `${p.sender} is typing...`;
        } else {
          typingIndicator.textContent = "";
        }
        break;
      }
      default:
        console.warn("Unknown action type:", res.type);
    }
  } catch (err) {
    console.error("Failed to parse inbound frame:", err);
  }
}

function main() {
  if (!window.WebSocket) return;

  const wsUrl = `ws://${window.location.host}${window.location.pathname}/ws`;
  const conn = new WebSocket(wsUrl);

  const chatSection = /** @type {HTMLElement} */ (
    document.querySelector("#chat-section")
  );
  const chatForm = /** @type {HTMLFormElement} */ (
    document.querySelector("#chat-form")
  );
  const input = /** @type {HTMLInputElement} */ (
    document.querySelector("#chat-message-input")
  );
  const chatReset = /** @type {HTMLElement} */ (
    document.querySelector("#chat-message-reset")
  );
  const typingIndicator = /** @type {HTMLElement} */ (
    document.querySelector("#typing-indicator")
  );

  let lastTypingSent = 0;

  // Event Listeners setup
  conn.onmessage = (e) => {
    handleIncomingMessage(e, chatSection, typingIndicator);
  };

  input.addEventListener("input", () => {
    const now = Date.now();
    // Throttle typing event to every 2 seconds
    if (now - lastTypingSent > 2000) {
      lastTypingSent = now;
      sendAction(conn, "typing", { is_typing: true });
    }
  });

  chatForm.addEventListener("submit", (e) => {
    e.preventDefault();
    const text = input.value.trim();
    if (!text) return;

    // Send chat message and clear typing state
    sendAction(conn, "chat", { message: text });
    sendAction(conn, "typing", { is_typing: false });

    chatForm.reset();
    lastTypingSent = 0;
  });

  chatReset.addEventListener("click", (e) => {
    e.preventDefault();
    chatSection.innerHTML = "";
  });

  // Highlight current room link
  const currentPath = window.location.pathname;
  document.querySelectorAll("header nav a").forEach((link) => {
    if (link.getAttribute("href") === currentPath) {
      link.setAttribute("aria-current", "page");
    }
  });
}

main();
