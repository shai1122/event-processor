const axios = require("axios");
const fs = require("fs");
const readline = require("readline");

const SERVER_URL = "http://localhost:8000/liveEvent";
const AUTH_SECRET = "secret";
const EVENTS_FILE = "events.jsonl";

const sendEvent = async (event) => {
  try {
    await axios.post(SERVER_URL, event, {
      headers: {
        Authorization: AUTH_SECRET,
      },
    });
    console.log("Event sent:", event);
  } catch (error) {
    console.error("Error sending event:", error);
  }
};

const processEvents = () => {
  const rl = readline.createInterface({
    input: fs.createReadStream(EVENTS_FILE),
    output: process.stdout,
    terminal: false,
  });

  rl.on("line", (line) => {
    const event = JSON.parse(line);
    sendEvent(event);
  });

  rl.on("close", () => {
    console.log("All events processed");
  });
};

processEvents();
