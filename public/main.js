function renderChatBox(username, msg) {
  return `
    <article>
      <header>
        <small>${username}</small>
      </header>
      <p>${msg}</p>
    </article>
  `;
}

function main() {
  if (!window.WebSocket) {
    throw new Error("WebSockets not supported");
  }

  const conn = new WebSocket("ws://" + document.location.host + "/ws");

  const chatSection = document.querySelector("#chat-section");
  const chatForm = document.querySelector("#chat-form");
  const input = document.querySelector("#chat-message-input");
  const chatReset = document.querySelector("#chat-message-reset");

  conn.onopen = () => {
    console.log("connected");
  };

  conn.onmessage = (event) => {
    const parts = event.data.split(": ");
    const html = renderChatBox(parts[0], parts[1]);
    chatSection.insertAdjacentHTML("beforeend", html);
  };

  conn.onerror = (error) => {
    console.error("WebSocket error:", error);
  };

  chatForm.addEventListener("submit", (e) => {
    e.preventDefault();

    const messageText = input.value.trim();
    if (!messageText) return;

    conn.send(messageText);

    chatForm.reset();
  });

  chatReset.addEventListener("click", (e) => {
    chatSection.innerHTML = "";
  });
}

main();
