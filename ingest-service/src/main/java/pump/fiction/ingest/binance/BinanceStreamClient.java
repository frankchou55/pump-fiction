package pump.fiction.ingest.binance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import pump.fiction.ingest.config.BinanceProperties;
import pump.fiction.ingest.kafka.TradePublisher;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Component
public class BinanceStreamClient implements WebSocket.Listener {

    private static final Logger log = LoggerFactory.getLogger(BinanceStreamClient.class);

    private final BinanceProperties props;
    private final AggTradeParser parser;
    private final TradePublisher publisher;
    private final HttpClient http = HttpClient.newHttpClient();

    // Collects a message that arrives in several pieces (see onText).
    // Safe without locks: the JDK calls the listener one message at a time.
    private final StringBuilder buffer = new StringBuilder();

    private volatile WebSocket webSocket;

    public BinanceStreamClient(BinanceProperties props, AggTradeParser parser, TradePublisher publisher) {
        this.props = props;
        this.parser = parser;
        this.publisher = publisher;
    }

    /** Opens the connection once the app has fully started. */
    @EventListener(ApplicationReadyEvent.class)
    public void connect() {
        URI uri = buildStreamUri();
        log.info("Connecting to {}", uri);
        http.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .buildAsync(uri, this)
                .whenComplete((_, err) -> {
                    if (err != null) {
                        log.error("Failed to connect to Binance", err);
                    }
                });
    }

    /** e.g. wss://stream.binance.us:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade */
    URI buildStreamUri() {
        String streams = props.symbols().stream()
                .map(s -> s.toLowerCase() + "@aggTrade")
                .collect(Collectors.joining("/"));
        return URI.create(props.streamUrl() + "?streams=" + streams);
    }

    // ---- WebSocket.Listener callbacks ----

    @Override
    public void onOpen(WebSocket ws) {
        this.webSocket = ws;
        log.info("Connected to Binance");
        ws.request(1); // GOTCHA 1: ask for the first message, or nothing ever arrives
    }

    @Override
    public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
        // GOTCHA 2: one message can arrive in several pieces; 'last' marks the final piece
        buffer.append(data);
        if (last) {
            long ingestTime = System.currentTimeMillis();
            String message = buffer.toString();
            buffer.setLength(0);
            handleMessage(message, ingestTime);
        }
        ws.request(1); // ask for the next piece/message (this is the backpressure)
        return null;   // null = "done with this data, the JDK can reuse its buffer"
    }

    private void handleMessage(String json, long ingestTime) {
        parser.parse(json, ingestTime).ifPresent(publisher::publish);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
        log.warn("Binance connection closed: {} {}", statusCode, reason);
        return null; // step 5 adds reconnect logic here
    }

    @Override
    public void onError(WebSocket ws, Throwable error) {
        log.error("Binance connection error", error); // and here
    }

    /** Close politely when the app shuts down. */
    @EventListener(ContextClosedEvent.class)
    public void disconnect() {
        WebSocket ws = this.webSocket;
        if (ws != null) {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown");
        }
    }
}