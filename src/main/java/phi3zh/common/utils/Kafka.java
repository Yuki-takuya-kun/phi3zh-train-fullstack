package phi3zh.common.utils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class Kafka {
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
