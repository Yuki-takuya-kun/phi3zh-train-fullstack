package common.kafka;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class Utils {
    /**
     * calculate the data size of given topic
     * @param client
     * @param topicName
     * @return the data size in a given topic
     * @throws Exception
     */
    public static long dataSizeInTopic(AdminClient client, String topicName) throws Exception{
        Map<String, TopicDescription> topics = client.describeTopics(Collections.singleton(topicName)).all().get();
        TopicDescription description = topics.get(topicName);
        Map<TopicPartition, OffsetSpec> earliestOffsetQuery = description.partitions().stream().collect(Collectors.toMap(
                item -> new TopicPartition(topicName, item.partition()), item->OffsetSpec.earliest()
        ));
        Map<TopicPartition, OffsetSpec> latestOffsetQuery = description.partitions().stream().collect(Collectors.toMap(
                item -> new TopicPartition(topicName, item.partition()), item->OffsetSpec.latest()
        ));
        Map<TopicPartition, ListOffsetsResultInfo> beginOffsets = client.listOffsets(earliestOffsetQuery).all().get();
        Map<TopicPartition, ListOffsetsResultInfo> endOffsets = client.listOffsets(latestOffsetQuery).all().get();
        long dataSize = endOffsets.values().parallelStream().mapToLong(ListOffsetsResultInfo::offset).sum() -
                    beginOffsets.values().parallelStream().mapToLong(ListOffsetsResultInfo::offset).sum();
        return dataSize;
    }
}
