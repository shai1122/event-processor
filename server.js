const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");

const app = express();
const PORT = 8000;
const AUTH_SECRET = "secret";
const SERVER_ID = process.env.SERVER_ID || "server1"; // Unique server instance ID
const EVENTS_DIR = path.join(__dirname, "events", SERVER_ID);
const EVENTS_FILE = path.join(EVENTS_DIR, "events.log");
const BUFFER_FILE = path.join(EVENTS_DIR, "events_buffer.log");
const MAX_FILE_SIZE = 100 * 1024 * 1024; // 100MB

if (!fs.existsSync(EVENTS_DIR)) {
  fs.mkdirSync(EVENTS_DIR, { recursive: true });
} else {
  console.log(`Directory already exists: ${EVENTS_DIR}`);
}

app.use(bodyParser.json());

const authenticate = (req, res, next) => {
  const authHeader = req.headers["authorization"];
  if (authHeader === AUTH_SECRET) {
    next();
  } else {
    res.status(403).send("Forbidden");
  }
};

// PostgreSQL connection pool
const pool = new Pool({
  user: "postgres",
  host: "localhost",
  database: "etl",
  password: "1234",
  port: 5432,
});

const isFileLocked = () =>
  fs.existsSync(path.join(EVENTS_DIR, "processor.lock"));

const rotateLogFile = (fileName) => {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const rotatedFileName = `${fileName}.${timestamp}`;
  fs.renameSync(fileName, rotatedFileName);
  console.log(`Rotated log file to ${rotatedFileName}`);
};

const writeEvent = (event) => {
  if (!fs.existsSync(EVENTS_DIR)) {
    console.log(`Creating directory before writing: ${EVENTS_DIR}`);

    fs.mkdirSync(EVENTS_DIR, { recursive: true });
    console.log("abefore");
  }

  const fileName = isFileLocked() ? BUFFER_FILE : EVENTS_FILE;
  fs.appendFile(fileName, JSON.stringify(event) + "\n", (err) => {
    if (err) {
      console.error("Error writing to file:", err);
    }
  });

  try {
    const stats = fs.statSync(fileName);
    if (stats.size >= MAX_FILE_SIZE) {
      rotateLogFile(fileName);
    }
  } catch (err) {
    console.error("Error getting file stats:", err);
  }
};

app.post("/liveEvent", authenticate, (req, res) => {
  const event = req.body;
  writeEvent(event);
  res.status(200).send("Event received");
});

app.get("/userEvents/:userid", async (req, res) => {
  const userId = req.params.userid;
  try {
    const result = await pool.query(
      "SELECT * FROM users_revenue WHERE user_id = $1",
      [userId]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).send("Internal Server Error");
  }
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
