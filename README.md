[![progress-banner](https://backend.codecrafters.io/progress/redis/4198420b-4799-497b-abf7-48076cf50ec6)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This is a starting point for C++ solutions to the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

In this challenge, you'll build a toy Redis clone that's capable of handling
basic commands like `PING`, `SET` and `GET`. Along the way we'll learn about
event loops, the Redis protocol and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Passing the first stage

The entry point for your Redis implementation is in `src/Server.cpp`. Study and
uncomment the relevant code, and push your changes to pass the first stage:

```sh
git commit -am "pass 1st stage" # any msg
git push origin master
```

That's all!

# Stage 2 & beyond

Note: This section is for stages 2 and beyond.

1. Ensure you have `cmake` installed locally
1. Run `./your_program.sh` to run your Redis server, which is implemented in
   `src/Server.cpp`.
1. Commit your changes and run `git push origin master` to submit your solution
   to CodeCrafters. Test output will be streamed to your terminal.

# Custom port

Pass `--port <number>` to run on a non-default port (useful when running multiple instances for replication):

```sh
./your_program.sh --port 7000
```

# Replica mode

Start a replica by pointing it at a master host/port:

```sh
./your_program.sh --port 6380 --replicaof "localhost 6379"
```

# Running tests

Build and run the unit tests (CMake/CTest):

```sh
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake
cmake --build build
ctest --test-dir build
```

# Architecture overview

- `src/Server.cpp` boots a non-blocking TCP server on port 6379, keeps per-client state (socket, read buffer, pending responses, transaction queue), and runs a `select`-based event loop that accepts clients, reads RESP frames, queues writes, and integrates the `BlockingManager` for list/stream blocking. `MULTI`/`EXEC` queuing lives here, and responses are pushed to per-client queues before being flushed.
- `src/RESP.cpp` contains a small RESP array parser used by the server when splitting incoming frames into command parts, while also supporting inline commands as a fallback.
- `src/Commands.cpp`/`.hpp` implement the Redis commands: strings (`PING`, `ECHO`, `SET` with `PX`, `GET`, `INCR`), lists (`LPUSH`, `RPUSH`, `LRANGE`, `LLEN`, `LPOP`, `BLPOP` helper hooks), transactions (`QUEUED` replies for `MULTI`/`EXEC`), type introspection (`TYPE`), and streams (`XADD`, `XRANGE`, `XREAD`). Command helpers can enqueue responses immediately or return them through the server’s sender callback.
- `src/Store.hpp` defines the in-memory store: a `Store` map from keys to polymorphic values (`StringValue` with expirations, `ListValue`, `StreamValue` with ordered entries). Helpers provide simple expiry checks and type-safe lookups used by the handlers.
- `src/BlockingManager.cpp`/`.hpp` tracks clients blocked on list pops or stream reads. It registers waiting FDs per key, wakes them when `LPUSH`/`RPUSH`/`XADD` add data, and handles timeouts by replying with `*-1`. It builds RESP replies for `BLPOP` and `XREAD` waiters before returning control to the server loop.

# Possible improvements (most important first)

1. Harden networking: handle partial sends/receives fully, close sockets on repeated errors, and add backpressure to avoid unbounded `pending_responses` growth.
2. Add command validation and richer RESP parsing (better errors, unified inline parsing) plus fuzz/unit tests for protocol edge cases.
3. Implement eviction/expiry cleanup and persistence (RDB/AOF) so expired keys are swept proactively and data survives restarts.
4. Replace `select` with `epoll`/`kqueue` for scalability and move block/unblock bookkeeping to be O(1) per wake-up.
5. Expand feature surface: AUTH, CONFIG/INFO, PSUBSCRIBE/SUBSCRIBE, and stream trimming (XTRIM) to mirror more Redis behaviors.
6. Improve observability: structured logs, metrics counters for commands/latency, and trace IDs per connection to debug blocking workflows.
