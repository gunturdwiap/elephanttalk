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

function main() {
  if (!window.WebSocket) return;

  const conn = new WebSocket("ws://" + document.location.host + "/ws");

  const chatSection = document.querySelector("#chat-section");
  const chatForm = document.querySelector("#chat-form");
  const input = document.querySelector("#chat-message-input");
  const chatReset = document.querySelector("#chat-message-reset");

  conn.onmessage = (event) => {
    try {
      console.log(event.data);
      const data = JSON.parse(event.data);
      const html = renderChatBox(data.sender, data.sent_at, data.message);
      chatSection.insertAdjacentHTML("beforeend", html);
      chatSection.scrollTop = chatSection.scrollHeight;
    } catch (err) {
      console.error("Invalid JSON:", err);
    }
  };

  chatForm.addEventListener("submit", (e) => {
    e.preventDefault();

    const text = input.value.trim();
    if (!text) return;

    const packet = {
      action: "chat",
      payload: {
        message: text,
      },
    };

    conn.send(JSON.stringify(packet));
    chatForm.reset();
  });

  chatReset.addEventListener("click", () => {
    chatSection.innerHTML = "";
  });
}

main();
