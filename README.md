# ms4-group-35

## Overview
In Milestone 4, we enhanced our distributed with the introduction of a user authentication system to improve the security and management of our key-value store.

## Running the System
To run the system, you need to be inside this ms4-group-35 directory.

First build the system by running `ant`

To start the ECS, open a terminal and run: `java -jar m2-ecs.jar`

To start a KV Server, open a terminal and run: `java -jar m2-server.jar -p <port number>`

To start a client, open a terminal and run: `java -jar m2-client.jar`

Client commands can be seen if you type `help` in the client. There is now additional functionality to `create` a new user, `login`, `logout`, and `reset_password`.

## Running Tests
Unit tests for M4 can be run using the `ant test` command in this ms4-group-35 directory.