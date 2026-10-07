package pump.fiction.ingest.binance;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import pump.fiction.ingest.model.AggTrade;
import pump.fiction.ingest.model.Side;

import java.math.BigDecimal;
import java.util.Optional;

@Component
public class AggTradeParser {
    private static final Logger log = LoggerFactory.getLogger(AggTradeParser.class);

    // One mapper, reused for every message: creating mappers is expensive
    private final ObjectMapper mapper = JsonMapper.builder().build();

    /**
     * Parses one combined-stream message: {"stream": "...", "data": {...}}.
     * Returns empty for anything that isn't an aggTrade or can't be parsed.
     */
    public Optional<AggTrade> parse(String json, long ingestTime) {
        try {
            JsonNode data = mapper.readTree(json).get("data");
            if (data == null || !"aggTrade".equals(data.path("e").asText())) {
                return Optional.empty(); // e.g. subscription replies like {"result":null,"id":1}
            }

            boolean buyerIsMaker = data.required("m").asBoolean();

            return Optional.of(new AggTrade(
                    data.required("s").asText(),
                    data.required("a").asLong(),
                    data.required("f").asLong(),
                    data.required("l").asLong(),
                    new BigDecimal(data.required("p").asText()),  // from the STRING, never a double
                    new BigDecimal(data.required("q").asText()),
                    buyerIsMaker ? Side.SELL : Side.BUY,             // maker was the buyer -> seller aggressed
                    data.required("T").asLong(),
                    data.required("E").asLong(),
                    ingestTime
            ));
        } catch (JacksonException | NumberFormatException e) {
            // Bad message: log and skip; never let it kill the listener
            log.warn("Skipping unparseable message: {}", json, e);
            return Optional.empty();
        }
    }
}
