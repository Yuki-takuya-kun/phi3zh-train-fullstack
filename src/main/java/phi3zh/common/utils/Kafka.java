package phi3zh.common.utils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class Kafka {
    private static AdminClient admin;

    private Kafka(){}

    public static AdminClient getAdmin(String bootstrapServers){
        if (admin == null){
            synchronized (AdminClient.class){
                if (admin == null){
                    Properties adminConfig = new Properties();
                    adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                    admin = AdminClient.create(adminConfig);
                }
            }
        }
        return admin;
    }

    public static Producer getStringProducer(String bootstrapServers){
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        Producer<String, String> producer = new KafkaProducer<>(producerProperties);
        return producer;
    }

    public static Consumer getStringConsumer(String bootstrapServers,
                                             String groupId,
                                             List<String> subscribes,
                                             int consumerPollNums){
        Properties consumerProp = new Properties();
        consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerPollNums);
        consumerProp.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer consumer = new KafkaConsumer(consumerProp);
        consumer.subscribe(subscribes);
        return consumer;
    }

    /**
     * calculate the data size of given topic
     * @param admin the Kafka admin client
     * @param topicName
     * @return the data size in a given topic
     * @throws Exception
     */
    public static long dataSizeInTopic(AdminClient admin, String topicName) throws Exception{

        Map<String, TopicDescription> topics = admin.describeTopics(Collections.singleton(topicName)).all().get();
        TopicDescription description = topics.get(topicName);
        Map<TopicPartition, OffsetSpec> earliestOffsetQuery = description.partitions().stream().collect(Collectors.toMap(
                item -> new TopicPartition(topicName, item.partition()), item->OffsetSpec.earliest()
        ));
        Map<TopicPartition, OffsetSpec> latestOffsetQuery = description.partitions().stream().collect(Collectors.toMap(
                item -> new TopicPartition(topicName, item.partition()), item->OffsetSpec.latest()
        ));
        Map<TopicPartition, ListOffsetsResultInfo> beginOffsets = admin.listOffsets(earliestOffsetQuery).all().get();
        Map<TopicPartition, ListOffsetsResultInfo> endOffsets = admin.listOffsets(latestOffsetQuery).all().get();
        long dataSize = endOffsets.values().stream().mapToLong(ListOffsetsResultInfo::offset).sum() -
                    beginOffsets.values().stream().mapToLong(ListOffsetsResultInfo::offset).sum();
        return dataSize;
    }

    public static long dataSizeUnConsume(AdminClient admin, String topicName, String groupId) throws Exception{
        Map<String, TopicDescription> topics = admin.describeTopics(Collections.singleton(topicName)).all().get();
        TopicDescription description = topics.get(topicName);
        Map<TopicPartition, Long> consumerOffsets = admin.listConsumerGroupOffsets(groupId,
                        new ListConsumerGroupOffsetsOptions()).partitionsToOffsetAndMetadata()
                .get()
                .entrySet().stream()
                .filter(e -> e.getKey().topic().equals(topicName))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
        Map<TopicPartition, OffsetSpec> latestOffsetQuery = description.partitions().stream().collect(Collectors.toMap(
                item -> new TopicPartition(topicName, item.partition()), item->OffsetSpec.latest()
        ));
        Map<TopicPartition, ListOffsetsResultInfo> endOffsets = admin.listOffsets(latestOffsetQuery).all().get();
        long dataSizeUnconsume = consumerOffsets.keySet().stream().collect(Collectors.toMap(
                tp -> tp,
                tp -> endOffsets.get(tp).offset() - consumerOffsets.get(tp)
        )).values().stream().mapToLong(Long::longValue).sum();
        return dataSizeUnconsume;
    }
}
