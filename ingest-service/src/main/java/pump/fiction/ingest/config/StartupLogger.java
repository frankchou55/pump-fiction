package pump.fiction.ingest.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class StartupLogger {
    private static final Logger log = LoggerFactory.getLogger(StartupLogger.class);

    private final BinanceProperties binance;

    public StartupLogger(BinanceProperties binance) {
        this.binance = binance;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void logConfig() {
        log.info("Binance stream URL: {}", binance.streamUrl());
        log.info("Symbols ({}): {}", binance.symbols().size(), binance.symbols());
    }
}
