package phi3zh.common.kafka;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class Utils {
    /**
     * calculate the data size of given topic
     * @param admin the kafka admin client
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
        long dataSize = endOffsets.values().parallelStream().mapToLong(ListOffsetsResultInfo::offset).sum() -
                    beginOffsets.values().parallelStream().mapToLong(ListOffsetsResultInfo::offset).sum();
        System.out.println(String.format("There are %d data in kafka", dataSize));
        return dataSize;
    }
}
