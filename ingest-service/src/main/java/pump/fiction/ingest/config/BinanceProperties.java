package pump.fiction.ingest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;
import java.util.List;

/**
 * Binds the "pumpfiction.binance" section of application.yml.
 * Records are immutable: values are set once at startup.
 */
@ConfigurationProperties(prefix = "pumpfiction.binance")
public record BinanceProperties(
        URI streamUrl,          // YAML "stream-url" -> streamUrl (relaxed binding)
        List<String> symbols    // YAML list -> List<String>
) {}
