package com.bjpractice.bets.config;


import com.bjpractice.events.GameFinishedEvent;
import com.bjpractice.events.PlayerDoubleEvent;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;


import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    // REHACTOR TIME!!!! WE HAVE TO CHANGE THIS INTO A WHOLE ASS JDK 25 SWITCH



    @Bean
    public ConsumerFactory<String, Object> consumerFactory(KafkaProperties properties) {
        final var consumerProperties = properties.buildConsumerProperties(null);
        consumerProperties.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return new DefaultKafkaConsumerFactory<>(consumerProperties);
    }

    // GAME FINISHED EVENT
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> gameFinishedEventContainerFactory(
            ConsumerFactory<String, Object> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setRecordFilterStrategy(record -> !record.value().getClass().equals(GameFinishedEvent.class));
        return factory;
    }


    // PLAYER DOUBLE EVENT
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> playerDoubleEventContainerFactory(
            ConsumerFactory<String, Object> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setRecordFilterStrategy(record -> !record.value().getClass().equals(PlayerDoubleEvent.class));
        return factory;
    }




    // Protocolo de emergencia para los listeners
    @Bean
    public DefaultErrorHandler errorHandler() {

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(0L, 1L));

        errorHandler.addNotRetryableExceptions(SerializationException.class);

        return errorHandler;
    }
}
