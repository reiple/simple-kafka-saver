import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 테스트용: 종료될 때까지 일정 간격으로 메시지를 계속 produce 합니다.
 * 10번째마다 개행/백슬래시/한글이 포함된 메시지를 보내 수신기의 이스케이프·UTF-8 처리를 확인합니다.
 */
public class TestProducer {
    public static void main(String[] args) throws Exception {
        String broker = System.getenv().getOrDefault("KAFKA_BROKER", "kafka:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "my-topic");
        long intervalMs = Long.parseLong(System.getenv().getOrDefault("PRODUCE_INTERVAL_MS", "200"));

        Properties config = new Properties();
        config.put("bootstrap.servers", broker);
        config.put("acks", "all");
        config.put("retries", "3");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        final KafkaProducer<String, String> producer = new KafkaProducer<>(config);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        System.out.println("Producing to " + topic + " @ " + broker + " every " + intervalMs + "ms");
        for (long id = 1; ; id++) {
            String value = "{\"id\":" + id + ",\"ts\":\"" + fmt.format(new Date()) + "\"}";
            if (id % 10 == 0) {
                value += "\n여러 줄 메시지\\백슬래시";
            }
            producer.send(new ProducerRecord<>(topic, String.valueOf(id), value), (meta, e) -> {
                if (e != null) {
                    System.err.println("send failed: " + e);
                }
            });
            if (id % 100 == 0) {
                System.out.println("sent " + id + " messages");
            }
            Thread.sleep(intervalMs);
        }
    }
}
