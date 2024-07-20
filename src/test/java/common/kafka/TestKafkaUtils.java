package kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.reflect.internal.Trees;

import java.util.Collections;
import java.util.Properties;
import common.kafka.Utils;

public class TestKafkaUtils {

    public static void main(String[] args){
        String bootstrapServers = "172.20.45.250:9092";
        String topicName = "test";
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AdminClient admin = AdminClient.create(config);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");


        NewTopic newTopic = new NewTopic(topicName, 1, (short)1);
        try {
            admin.createTopics(Collections.singleton(newTopic)).all().get();
            Producer<String, String> producer = new KafkaProducer<>(props);
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "key-" + i, "value-" + i);
                producer.send(record).get();
                long size = Utils.dataSizeInTopic(admin, topicName);
                System.out.println(size);
            }
            props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            admin.deleteTopics(Collections.singleton(topicName)).all().get();
            System.out.println("Topic deleted: " + topicName);

        } catch (Exception e){
            e.printStackTrace();

        }

    }


}
