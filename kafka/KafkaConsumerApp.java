package com.i2i.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerApp {

    private static final String TOPIC         = "orders";
    private static final String BROKER        = "cb140f2b0bd418.lhr.life:80";
    private static final String CONSUMER_GROUP = "order-consumer-group";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");

        ObjectMapper mapper = new ObjectMapper();

        System.out.println("[CONSUMER] Listening on topic: " + TOPIC);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));

            // Uygulamanın kapanmasını önlemek için sonsuz döngüye alıyoruz
            while (true) {
                // Her 2 saniyede bir yeni mesaj var mı diye broker'ı yoklar
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));

                if (records.isEmpty()) {
                    continue; // Mesaj yoksa döngünün başına dön ve beklemeye devam et
                }

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Order order = mapper.readValue(record.value(), Order.class);
                        System.out.printf("[CONSUMER] Received ← partition=%d, offset=%d | %s%n",
                                record.partition(), record.offset(), order);
                    } catch (Exception e) {
                        System.err.println("[CONSUMER] Mesaj parse edilemedi: " + record.value());
                    }
                }
            }
        }
    }
}