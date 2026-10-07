package pump.fiction.ingest.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import pump.fiction.ingest.config.KafkaTopicConfig;
import pump.fiction.ingest.model.AggTrade;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Component
public class TradePublisher {
    private static final Logger log = LoggerFactory.getLogger(TradePublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;

    public TradePublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper) {
        this.kafka = kafka;
        this.mapper = mapper;
    }

    public void publish(AggTrade trade) {
        String json;
        try {
            json = mapper.writeValueAsString(trade);
        } catch (JacksonException e) {
            log.error("Could not serialize {}", trade, e);
            return;
        }

        kafka.send(KafkaTopicConfig.TRADES_TOPIC, trade.symbol(), json)
                .whenComplete((result, err) -> {
                    if (err != null) {
                        log.error("Failed to publish {} #{}", trade.symbol(), trade.aggTradeId(), err);
                    }
                });
    }
}
