package pump.fiction.detector.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import pump.fiction.detector.model.AggTrade;
import pump.fiction.detector.stats.TradeCounter;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Component
public class TradeListener {
    private static final Logger log = LoggerFactory.getLogger(TradeListener.class);

    private final ObjectMapper mapper;            // tools.jackson.databind.ObjectMapper (Jackson 3)
    private final TradeCounter counter;

    public TradeListener(ObjectMapper mapper, TradeCounter counter) {   // Boot provides one; check what autocomplete offers
        this.mapper = mapper;
        this.counter = counter;
    }

    @KafkaListener(topics = "trades")
    public void onTrade(ConsumerRecord<String, String> record) {
        try {
            AggTrade trade = mapper.readValue(record.value(), AggTrade.class);
            counter.onTrade(trade);
            log.debug("{} price={} qty={} notional={} side={} p={} o={}",
                    trade.symbol(),
                    trade.price().toPlainString(),
                    trade.quantity().toPlainString(),
                    trade.notional().toPlainString(),
                    trade.aggressor(),
                    record.partition(),
                    record.offset());
        } catch (JacksonException e) {
            log.warn("Skipping unparseable record p={} o={}: {}",
                    record.partition(), record.offset(), e.getMessage());
        }
    }
}
