package pump.fiction.detector.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;

/**
 * One aggregated trade from Binance, in our own domain terms.
 * Times are epoch milliseconds.
 */
public record AggTrade(
        String symbol,
        long aggTradeId,      // Binance "a": consecutive per symbol, used for gap detection
        long firstTradeId,    // Binance "f"
        long lastTradeId,     // Binance "l"
        @JsonFormat(shape = JsonFormat.Shape.STRING) BigDecimal price,     // Binance "p" (string -> exact decimal)
        @JsonFormat(shape = JsonFormat.Shape.STRING) BigDecimal quantity,  // Binance "q"
        Side aggressor,       // derived from Binance "m"
        long tradeTime,       // Binance "T": when it happened; windows use this
        long eventTime,       // Binance "E": when Binance sent it
        long ingestTime       // our clock: when we received it
) {
    /** Traded value in quote currency (e.g. USDT). */
    public BigDecimal notional() {
        return price.multiply(quantity);
    }

    /** How long the message took to reach us, in ms (includes any clock skew). */
    public long feedLatencyMs() {
        return ingestTime - eventTime;
    }
}