package pump.fiction.detector.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import pump.fiction.detector.model.AggTrade;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Component
public class TradeCounter {
    private static final Logger log = LoggerFactory.getLogger(TradeCounter.class);
    private static final long MINUTE_MS = 60_000L;
    private static final DateTimeFormatter HH_MM =
            DateTimeFormatter.ofPattern("HH:mm").withZone(ZoneId.systemDefault());

    // The minute we are currently counting, per symbol.
    // NOT thread-safe: fine while exactly one thread calls onTrade().
    // TODO (later): what must change at listener concurrency > 1, or with 2 instances?
    private final Map<String, MinuteBucket> current = new HashMap<>();
    private long lateDropped = 0;

    public void onTrade(AggTrade trade) {
        long minute = trade.tradeTime() / MINUTE_MS;     // same number for every trade in one minute
        MinuteBucket bucket = current.get(trade.symbol());

        if (bucket == null) {
            // first trade we've seen for this symbol (this minute may be partial)
            current.put(trade.symbol(), new MinuteBucket(minute));
        } else if (minute == bucket.minute) {
            bucket.count++;
        } else if (minute > bucket.minute) {
            // TODO 1: the minute rolled over, so log the finished bucket and start a new one
            log.info(describe(trade.symbol(), bucket));
            current.put(trade.symbol(), new MinuteBucket(minute));   // starts at count = 1: this trade
        } else {
            // TODO 2: late trade, its minute bucket has already been replaced
            lateDropped++;
            if (lateDropped % 50 == 1) {
                log.info("Late trades dropped so far: {} (latest: {} is {} min behind)",
                        lateDropped, trade.symbol(), bucket.minute - minute);
            }
        }
    }

    private static String describe(String symbol, MinuteBucket b) {
        return symbol + " " + HH_MM.format(Instant.ofEpochMilli(b.minute * MINUTE_MS))
                + " -> " + b.count + " trades";
    }

    private static final class MinuteBucket {
        final long minute;
        long count = 1;               // created by a trade, so it starts at 1
        MinuteBucket(long minute) { this.minute = minute; }
    }
}
