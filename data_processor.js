const fs = require("fs");
const readline = require("readline");
const { Pool } = require("pg");
const path = require("path");

const SERVER_ID = process.env.SERVER_ID || "server1"; // Unique server instance ID
const EVENTS_DIR = path.join(__dirname, "events", SERVER_ID);
const EVENTS_FILE = path.join(EVENTS_DIR, "events.log");
const BUFFER_FILE = path.join(EVENTS_DIR, "events_buffer.log");
const POSITION_FILE = path.join(EVENTS_DIR, "last_position.txt");
const LOCK_FILE = path.join(EVENTS_DIR, "processor.lock");
const CHUNK_SIZE = 1024 * 1024; // 1MB

const pool = new Pool({
  user: "postgres",
  host: "localhost",
  database: "etl",
  password: "1234",
  port: 5432,
});

const connectToDatabase = async () => {
  try {
    const client = await pool.connect();
    console.log("Connected to the database successfully");
    client.release();
  } catch (err) {
    console.error("Failed to connect to the database:", err.stack);
  }
};

const acquireFileLock = () => {
  if (fs.existsSync(LOCK_FILE)) {
    console.log("Lock file exists. Checking if process is still running...");
    const pid = fs.readFileSync(LOCK_FILE, "utf8");
    try {
      process.kill(parseInt(pid), 0);
      throw new Error("Data processor is already running");
    } catch (err) {
      if (err.code === "ESRCH") {
        console.log("Stale lock file detected. Removing it.");
        fs.unlinkSync(LOCK_FILE);
      } else {
        throw new Error("Data processor is already running");
      }
    }
  }
  fs.writeFileSync(LOCK_FILE, process.pid.toString(), "utf8");
};

const releaseFileLock = () => {
  if (fs.existsSync(LOCK_FILE)) {
    fs.unlinkSync(LOCK_FILE);
  }
};

const acquireDbLock = async (client, userId) => {
  const lockId = BigInt("0x" + Buffer.from(userId).toString("hex"));
  await client.query("SELECT pg_advisory_lock($1)", [lockId]);
};

const releaseDbLock = async (client, userId) => {
  const lockId = BigInt("0x" + Buffer.from(userId).toString("hex"));
  await client.query("SELECT pg_advisory_unlock($1)", [lockId]);
};

const updateRevenue = async (client, userId, value) => {
  try {
    await acquireDbLock(client, userId);
    await client.query("BEGIN");
    await client.query(
      `
      INSERT INTO users_revenue (user_id, revenue)
      VALUES ($1, $2)
      ON CONFLICT (user_id)
      DO UPDATE SET revenue = users_revenue.revenue + EXCLUDED.revenue;
    `,
      [userId, value]
    );
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Error updating revenue:", err);
  } finally {
    await releaseDbLock(client, userId);
  }
};

const processEvents = async (file) => {
  let startPosition = 0;

  try {
    fs.mkdirSync(EVENTS_DIR, { recursive: true });
    console.log(`Directory ensured: ${EVENTS_DIR}`);
  } catch (err) {
    console.error("Error ensuring directory:", err);
  }

  if (file === EVENTS_FILE && fs.existsSync(POSITION_FILE)) {
    const posData = fs.readFileSync(POSITION_FILE, "utf8");
    startPosition = parseInt(posData, 10);
  }

  console.log(`Starting to process events from position: ${startPosition}`);

  if (!fs.existsSync(file)) {
    console.error(`File not found: ${file}`);
    return;
  }

  const client = await pool.connect();
  let currentOffset = startPosition;

  try {
    let hasLines = false;

    const stream = fs.createReadStream(file, { start: startPosition });
    const rl = readline.createInterface({
      input: stream,
      crlfDelay: Infinity,
    });

    for await (const line of rl) {
      hasLines = true;
      console.log(`Processing line: ${line}`);
      const event = JSON.parse(line);
      const value = event.name === "add_revenue" ? event.value : -event.value;
      await updateRevenue(client, event.userId, value);

      currentOffset += Buffer.byteLength(line) + 1; // +1 for the newline character

      // Save the current position periodically to handle large files
      if (file === EVENTS_FILE && currentOffset - startPosition >= CHUNK_SIZE) {
        fs.writeFileSync(POSITION_FILE, currentOffset.toString(), "utf8");
        startPosition = currentOffset;
      }
    }

    if (file === EVENTS_FILE) {
      fs.writeFileSync(POSITION_FILE, currentOffset.toString(), "utf8");
    }

    if (!hasLines) {
      console.log(`No lines to process in file: ${file}`);
    }

    console.log(
      `All events in ${file} processed up to position:`,
      currentOffset
    );
  } finally {
    client.release();
  }
};

const processRotatedFiles = async () => {
  const files = fs
    .readdirSync(EVENTS_DIR)
    .filter(
      (file) =>
        file.startsWith(path.basename(EVENTS_FILE) + ".") &&
        !file.endsWith(".lock")
    );
  for (const file of files) {
    await processEvents(path.join(EVENTS_DIR, file));
    fs.unlinkSync(path.join(EVENTS_DIR, file));
  }
};

const main = async () => {
  try {
    acquireFileLock();
    await connectToDatabase();

    await processEvents(EVENTS_FILE);
    await processRotatedFiles();

    if (fs.existsSync(BUFFER_FILE)) {
      await processEvents(BUFFER_FILE);
    }
  } catch (error) {
    console.error("Error:", error.message);
  } finally {
    releaseFileLock();
    await pool.end();
  }
};

main().catch((error) => {
  console.error("Unhandled error:", error);
  releaseFileLock();
  pool.end();
});
