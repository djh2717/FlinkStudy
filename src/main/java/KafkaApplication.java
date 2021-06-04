import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author Djh on  2021/5/28 11:07
 * @E-Mail 1544579459.djh@gmail.com
 */
public class KafkaApplication {
    public static void main(String[] args) throws InterruptedException {
        new Thread(() -> producer()).start();

//        consumer();
    }

    private static void producer() {
        // 1. 生产者配置
        Properties properties = new Properties();
        // 指定kafka地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 指定ack等级
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 指定重试次数，即生产者发送数据后没有收到ack应答时的重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 指定批次大小 16k = 16 * 1024
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        // 指定等待时间，单位毫秒
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // 指定RecordAccumulator缓冲区大小 32m = 32 * 1024 * 1024
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 指定k-v序列化规则
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        // 2. 创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String[] s = new String[]{"a", "b", "c", "d", "e", "f", "g", "i"};
        while (true){
            int index = ((int) (Math.random() * 100)) % s.length;
            ProducerRecord<String, String> record = new ProducerRecord<>("TestComposeTopic", s[index]);
            producer.send(record);
        }
//        producer.close();
        // 3. 准备数据
        // 4. 发送数据（不带回调）
        // 5. 关闭连接
    }


    private static void consumer() throws InterruptedException {
        // 1. 消费者配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 提交offset的时间，单位ms，即1秒钟提交一次
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // 指定k-v反序列化规则
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 指定消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "My-Consumer-Group");

        // 2. 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅主题
        consumer.subscribe(Collections.singletonList("TestComposeTopic"));
        while (true) {
            // 拉取数据，指定轮询时间为1秒
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.toString());
            }
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
