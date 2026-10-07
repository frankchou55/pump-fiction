package pump.fiction.ingest.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    public static final String TRADES_TOPIC = "trades";

    @Bean
    public NewTopic tradesTopic() {
        return TopicBuilder.name(TRADES_TOPIC)
                .partitions(6)
                .replicas(1)
                .build();
    }
}
