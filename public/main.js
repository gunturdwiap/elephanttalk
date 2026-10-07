function main() {
  if (!window["WebSocket"]) throw new Error("no ws");

  conn = new WebSocket("ws://" + document.location.host + "/ws");

  document
    .querySelector("#btn")
    .addEventListener("click", () => conn.send("clicked"));

  conn.onopen = (e) => {
    console.log("close", e);
    conn.send("Sup");
  };

  conn.onclose = (e) => {
    console.log("close", e);
  };

  conn.onmessage = (e) => {
    console.log("message", e);
    const p = document.createElement("p");
    p.innerHTML = e.data;
    document.querySelector("body").appendChild(p);
  };

  conn.onerror = (e) => {
    console.log("err", e);
  };
}

main();
