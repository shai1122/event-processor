# Event Processor

## Overview

The Event Processor is a Node.js application that processes events from log files and updates user revenue information in a PostgreSQL database. It includes both a server component that receives live events and a data processor component that processes events from log files.

## Prerequisites

- Node.js (v14 or later)
- PostgreSQL (v12 or later)

## Installation

1. Clone the repository:
   git clone https://github.com/shai1122/event-processor.git

Install dependencies:

npm install

Create a PostgreSQL database and user.
Update the PostgreSQL credentials in the server.js and dataProcessor.js files.

Running the Server:

SERVER_ID=server1 node server.js

The server will listen on port 8000 by default. You can send events to the server using the /liveEvent endpoint.

Running the Data Processor:
SERVER_ID=server1 node dataProcessor.js

The data processor will read events from the log files and update the database.

Endpoints

POST /liveEvent
Description: Receives a single event from the client and appends it to a local file.
Headers:
Authorization: Must be set to secret.

{
"userId": "string",
"name": "add_revenue" | "subtract_revenue",
"value": integer
}

GET /userEvents/
Description: Returns all data for a given user from the database table.
Parameters:
userid: The user ID to retrieve events for.
