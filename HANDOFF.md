# HANDOFF: Pump Fiction

Read this first in a new session. Last updated: 2026-10-08.

## What this is
A real-time crypto **pump-and-dump detector** that Frank is building **himself** to relearn Angular/NgRx and learn Kafka.
The question it answers: *"Which coins are showing pump-like behavior right now, and how often do those surges reverse?"*

## How we work (important)
- **Frank writes the code. Claude coaches**: explains concepts, proposes structure, reviews, debugs, asks "what happens if…" questions.
- When Frank asks for boilerplate, give a small skeleton **with file locations and a short explanation of each decision**, not a finished feature. Keep him in the driver's seat.
- Go slow; understanding beats speed. One step at a time, each ending in something visible.
- Frank commits and pushes himself.
- Market Desk (`~/Workspace/Projects/marketdesk`) was mostly AI-written and is **not** this project; don't mix them.

## Architecture (target)
```
Binance.US @aggTrade WS ─► ingest-service ─► Kafka "trades" ─► detector-service (Kafka Streams)
                                                                  ├─► "candles", "alerts", "outcomes" topics
                                                                  ▼
                                   sink-service ─► Supabase (Postgres)      gateway-service ─► WebSocket ─► Angular/NgRx UI
```
- Raw trades live **only in Kafka** (replay/backtest). Supabase stores results: 1-min candles, alerts, outcomes (price +5/+15/+30 min), rule settings/versions.
- Hosting: develop locally now; later move the whole `docker compose` stack to one always-on US-region VM (Oracle free ARM or a small VPS), Angular on Cloudflare, Supabase hosted. Undecided; decide when the detector works. (Considered a self-hosted Kafka VM on the home Proxmox OptiPlex, reached over Tailscale by services in the cloud; shelved 2026-10-08. Local Colima stays the dev setup.)

## Decisions made (and why)
| Topic | Decision |
|---|---|
| Feed | Binance.**US** (binance.com blocks US). `@aggTrade` streams, not `@trade`: agg ids match the REST backfill endpoint. |
| Money types | `BigDecimal` built from Binance's **strings**; never `new BigDecimal(double)`; `compareTo` not `equals`; divide with a `RoundingMode`. Published to Kafka as JSON **strings** (`@JsonFormat(shape = STRING)`). |
| Timestamps | Window on trade time `T`. Keep `E` (event) and our `ingestTime` for latency. Never window on receive time (a reconnect burst would look like a pump). |
| Gaps | Detect by consecutive agg trade ids per symbol; backfill via REST by id range. Windows with a known gap: hold (bounded timeout) then judge; mark alerts `final`/`incomplete`; upsert on `(symbol, window_start, rule_version)`. |
| Kafka keys | Key = symbol (per-symbol ordering). `trades` topic: 6 partitions, replicas 1 (local). |
| Producer | `acks=all`, `compression-type=lz4`, `linger.ms=20`, `max.block.ms=5000`. Do no work on the WebSocket thread beyond parse + `send()`. |
| DB | Supabase via **Session pooler** (direct is IPv6-only; transaction pooler breaks prepared statements). Secrets in env vars / git-ignored `application-local.yml`. `ddl-auto: validate`; schema via SQL/Flyway. Idempotent upserts; batch writes. JPA fine for reads; consider `JdbcTemplate` for `ON CONFLICT` batch writes. Free tier pauses after ~1 week idle. |
| Build | Gradle **Kotlin DSL**, Spring Boot **4.1.1**, **Java 25**, JAR, YAML config. No Lombok (records instead). No Spring Web in ingest (lean). |
| Sharing `AggTrade` | **Copied** into detector (`model/AggTrade`, `model/Side`) on purpose, to feel the drift. Add a shared `common` Gradle module when the two copies bite. |
| Detector consumer | Group `detector`, `auto-offset-reset: earliest` (only applies to a group with no committed offsets), String deserializers, then parse JSON into `AggTrade` ourselves. Unparseable record: log partition/offset and skip (placeholder for a dead-letter topic). |
| Per-minute counter | `TradeCounter`: one current bucket per symbol, minute = `tradeTime / 60_000`. Logs the finished minute when a later-minute trade arrives; late trades are counted in `lateDropped`, not applied. In-memory `HashMap`, single-threaded only. |

## Current state
**Done (ingest-service, phase 1 producer half):** Kafka runs in Docker; `ingest-service` connects to Binance.US, parses `@aggTrade` into `AggTrade` records, and publishes JSON to Kafka `trades` keyed by symbol. Verified with the console consumer.

```
pump-fiction/
├── docker-compose.yml          # apache/kafka:4.1.0, single node KRaft, 9092 published; no volume (down = data lost)
├── .gitignore                  # .idea/ target/ build/ .DS_Store *.env application-local.yml
├── README.md                   # empty, to be written
└── ingest-service/             # package pump.fiction.ingest
    ├── build.gradle.kts        # actuator, kafka starter, jackson-databind (Jackson 3), devtools; bootRun --enable-native-access
    └── src/main/
        ├── resources/application.yaml   # keep-alive, kafka producer, pumpfiction.binance.{stream-url,symbols}
        └── java/pump/fiction/ingest/
            ├── IngestApplication.java   # @ConfigurationPropertiesScan
            ├── config/   BinanceProperties (record), JacksonConfig (shared ObjectMapper), KafkaTopicConfig (trades topic), StartupLogger (temp, deletable)
            ├── binance/  BinanceStreamClient (JDK WebSocket, request(1), partial-message buffer), AggTradeParser (Optional, never throws)
            ├── kafka/    TradePublisher (KafkaTemplate<String,String>, async send)
            └── model/    AggTrade (record, notional(), feedLatencyMs()), Side (BUY/SELL from "m")
```

**Done (detector-service, phase 1 consumer half):** a second, independent Gradle build next to ingest. `@KafkaListener` on `trades` (group `detector`) parses each JSON record into `AggTrade` and feeds `TradeCounter`, which logs one line per symbol per minute (e.g. `BTCUSDT 08:02 -> 412 trades`). Verified running against live Binance.US data.

```
detector-service/               # package pump.fiction.detector
├── build.gradle.kts            # actuator, kafka starter, jackson-databind (Jackson 3); bootRun --enable-native-access
└── src/main/
    ├── resources/application.yaml   # kafka consumer: group-id detector, earliest, String deserializers
    └── java/pump/fiction/detector/
        ├── DetectorApplication.java
        ├── config/  JacksonConfig (ObjectMapper bean; same as ingest)
        ├── kafka/   TradeListener (ConsumerRecord<String,String> -> AggTrade -> TradeCounter)
        ├── model/   AggTrade, Side (copied from ingest)
        └── stats/   TradeCounter (per-symbol minute buckets on tradeTime)
```

Observed and understood: the same symbol always lands on the same partition; offsets increase by 1 per partition; a restarted detector resumes from its committed offsets; stopping it while ingest runs builds consumer **lag** (`kafka-consumer-groups.sh --describe --group detector`).

## Environment gotchas already solved
- Container runtime is **Colima** (Homebrew docker CLI). Run `colima start` before Docker; `docker compose` plugin is wired via `~/.docker/config.json` → `"cliPluginsExtraDirs": ["/opt/homebrew/lib/docker/cli-plugins"]`.
- Without Spring Web the JVM would exit after startup → `spring.main.keep-alive: true`.
- Spring Boot 4 = **Jackson 3**: imports are `tools.jackson.*` (annotations stay `com.fasterxml.jackson.annotation`). No Jackson 2 on the classpath. Some node methods renamed (`asString` vs `asText`; use autocomplete).
- lz4 native-access warning → `--enable-native-access=ALL-UNNAMED` (in `bootRun`; also add as VM option in IntelliJ run config).
- Start order: `colima start` → `docker compose up -d` (from repo root) → run ingest (Kafka must be up: the `NewTopic` bean connects at boot) → run detector.
- Console helper: `k() { docker exec -it kafka /opt/kafka/bin/"$@"; }` then e.g. `k kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trades --property print.key=true --property print.partition=true`.
- **No `ObjectMapper` bean by default.** Only `jackson-databind` is on the classpath (no Boot JSON starter), so Boot does not create one. Both services define it in `config/JacksonConfig` (`JsonMapper.builder().build()`). Error if forgotten: "required a bean of type 'tools.jackson.databind.ObjectMapper'".
- A `@KafkaListener` keeps the JVM alive, so detector does not need `spring.main.keep-alive`.
- Check the broker with `docker ps` (container `kafka`); `colima status` / `colima start` if the Docker daemon is unreachable. No volume in compose: `down` or a recreated container loses the topic data.

## Open questions for Frank
- Did he run the "stop Kafka while ingest runs" experiment? What happened to the socket and to trades during the outage? (Sets the urgency of hardening ingest.)
- Multi-instance experiment not done yet: start 2 detector instances. What happens to partition assignment, and to the per-minute counts (each instance sees only some partitions)? This is why the in-memory counter does not scale.
- At-least-once: if detector crashes after handling a record but before its offset is committed, what happens on restart, and what does that mean for the later DB writes (idempotent upserts)?
- Still unanswered from the Kafka exercises: 4 consumers on 3 partitions in one group, what happens?

## Known limitations of detector today (deliberate, revisit)
- `TradeCounter` is single-threaded in-memory; it breaks with listener concurrency > 1 or multiple instances.
- A finished minute is logged only when the next trade for that symbol arrives (quiet symbols log late, and minutes with zero trades log nothing).
- Late trades are counted in `lateDropped` and not applied; first minute after startup is partial.
- Bad JSON is logged and skipped (no dead-letter topic).
- `AggTrade`/`Side`/`JacksonConfig` are duplicated between the two services.

## Next steps (in order; recommended path)
1. **Harden ingest** (recommended next): reconnect with backoff, staleness watchdog, gap detection + REST backfill (`/api/v3/aggTrades` by id), `/health` via JDK `HttpServer` + Actuator `HealthEndpoint`, optional Kafka heartbeat topic. Remove per-trade logging; log counts. Do the outage experiment first, since its result drives the design.
2. **Phase 2 "the brain":** Kafka Streams 1-min windows on trade time with grace period (replaces the in-memory `TradeCounter`), baselines, surge rule -> `alerts`; `suppress(untilWindowCloses)` for hold-off; replay/injection tool + `TopologyTestDriver` tests; outcome tracking; sink-service -> Supabase.
3. **Phase 3 "the face":** gateway-service (WebSocket, per-client conflation) + Angular/NgRx dashboard (alert feed, symbol detail, outcomes table).
4. Later: Docker volume for Kafka data, dockerize services (`bootBuildImage` or Dockerfile), second listener for in-Docker clients, shared `common` module if the copies hurt, hosting, README with measured numbers.
