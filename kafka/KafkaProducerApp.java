package com.i2i.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerApp {

    private static final String TOPIC  = "orders";
    private static final String BROKER = "cb140f2b0bd418.lhr.life:80";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Order[] orders = {
                new Order("ORD-001", "Laptop",     1,  1299.99),
                new Order("ORD-002", "Mouse",      2,    49.98),
                new Order("ORD-003", "Keyboard",   1,    89.99),
                new Order("ORD-004", "Monitor",    2,   599.98),
                new Order("ORD-005", "Headphones", 1,   149.99),
            };

            for (Order order : orders) {
                String json = mapper.writeValueAsString(order);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, order.getOrderId(), json);

                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        System.err.println("Failed to send: " + ex.getMessage());
                    } else {
                        System.out.printf("[PRODUCER] Sent → topic=%s, partition=%d, offset=%d | %s%n",
                                metadata.topic(), metadata.partition(), metadata.offset(), json);
                    }
                });
            }

            producer.flush();
            System.out.println("[PRODUCER] All messages sent.");
        }
    }
}
