package phi3zh.common.kafka.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import phi3zh.common.utils.Kafka;
import phi3zh.config.CommonConfig;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.Properties;

@SpringBootTest(classes = CommonConfig.class)
public class TestKafkaUtils {

    private String bootStrapServers;
    private String topicName = "KafkaUtilTest";
    private AdminClient client;

    @Autowired
    public TestKafkaUtils(CommonConfig commonConfig){
        this.bootStrapServers = commonConfig.getBootStrapServers();
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootStrapServers);
        this.client = AdminClient.create(config);

    }

    @Test
    public void testDatasizeQuery(){
        String topicName = "TestDatasizeQuery";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootStrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.Kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.Kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            this.client.createTopics(Collections.singleton(newTopic)).all().get();
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "key-" + i, "value-" + i);
                producer.send(record).get();
                long size = Kafka.dataSizeInTopic(this.client, topicName);
                assertEquals(size, i+1);
            }
        } catch (Exception e){
            e.printStackTrace();
            Assertions.fail("Test failed due to exception: " + e.getMessage());
        } finally {
            try {
                this.client.deleteTopics(Collections.singletonList(topicName)).all().get();
            } catch (Exception e){
                e.printStackTrace();
                Assertions.fail("Fail to delete topic: " + e.getMessage());
            }
        }
    }
}
