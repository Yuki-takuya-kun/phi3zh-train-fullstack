package phi3zh.dataconverter.controller;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RSemaphore;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import phi3zh.common.utils.Kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public abstract class Controller {

    protected Properties kafkaConfig;
    protected Config redisConfig;

    protected List<Triple<String, Integer, Short>> kafkaTopics;
    protected List<Pair<String, Boolean>> booleanBucketWithDefaults;
    protected List<Pair<String, Integer>> semaphoreWithDefaultValues;

    protected AdminClient kafkaClient;
    protected RedissonClient redissonClient;

    private List<Pair<Future, Triple<String, String, String>>> monitorAttrs = new ArrayList<>();

    public abstract void run();

    protected void setKafkaConfig(Properties kafkaConfig){
        this.kafkaConfig = kafkaConfig;
    }

    protected void setRedisConfig(Config redisConfig){
        this.redisConfig = redisConfig;
    }

    protected void setKafkaTopics(List<Triple<String, Integer, Short>> kafkaTopics){
        this.kafkaTopics = kafkaTopics;
    }

    protected void setBooleanBucketWithDefaults(List<Pair<String, Boolean>> booleanBucketWithDefaults){
        this.booleanBucketWithDefaults = booleanBucketWithDefaults;
    }

    protected void setSemaphoreWithDefaultValues(List<Pair<String, Integer>> semaphoreWithDefaultValues){
        this.semaphoreWithDefaultValues = semaphoreWithDefaultValues;
    }

    protected void initializeKafka(){
        kafkaClient = AdminClient.create(kafkaConfig);
        kafkaClient.deleteTopics(kafkaTopics.stream().map(Triple::getLeft).collect(Collectors.toList()));
        List<NewTopic> newTopics = kafkaTopics.stream().map(topic ->
                        new NewTopic(topic.getLeft(), topic.getMiddle(), topic.getRight()))
                .collect(Collectors.toList());
        kafkaClient.createTopics(newTopics);
    }

    protected void shutdownKafka(){
        kafkaClient.deleteTopics(kafkaTopics.stream().map(Triple::getLeft).collect(Collectors.toList()));
        kafkaClient.close();
    }

    protected void initializeRedis(){
        redissonClient = Redisson.create(redisConfig);
        booleanBucketWithDefaults.stream().forEach(pair -> {
            RBucket<Boolean> booleanRBucket = redissonClient.getBucket(pair.getLeft());
            booleanRBucket.set(pair.getRight());
        });
        semaphoreWithDefaultValues.stream().forEach(pair->{
            RSemaphore semaphore = redissonClient.getSemaphore(pair.getLeft());
            semaphore.trySetPermits(pair.getRight());
        });
    }

    protected void shutdownRedis(){
        booleanBucketWithDefaults.stream().forEach(pair -> redissonClient.getBucket(pair.getLeft()).delete());
        semaphoreWithDefaultValues.stream().forEach(pair -> redissonClient.getSemaphore(pair.getLeft()).delete());
        redissonClient.shutdown();
    }

    protected void addMonitorAttr(Future future, String bucketName, String topicName, String groupId){
        if (topicName == null && bucketName != null || topicName != null && bucketName == null){
            throw new RuntimeException("topicName and bucketName should both null");
        }
        this.monitorAttrs.add(Pair.of(future, Triple.of(bucketName, topicName, groupId)));
    }

    protected void runMonitor(){
        boolean allDone = false;
        while (!allDone){
            boolean allDoneCp = true;
            for (Pair<Future, Triple<String, String, String>> elem: this.monitorAttrs){
                Triple<String, String, String> triple = elem.getRight();
                boolean done = checkDoneTrigger(elem.getLeft(), triple.getLeft(), triple.getMiddle(), triple.getRight());
                allDoneCp = done && allDoneCp;
            }
            allDone = allDoneCp;
        }
    }

    private boolean checkDoneTrigger(Future future, String bucketName, String topicName, String groupId){
        try {
            if (!future.isDone()){
                return false;
            }
            else if (topicName == null && bucketName == null){
                return true;
            }
            else if (topicName != null && bucketName != null && Kafka.dataSizeUnConsume(this.kafkaClient, topicName, groupId)==0){
                RBucket<Boolean> bucket = redissonClient.getBucket(bucketName);
                bucket.set(true);
                return true;
            } else {
                return false;
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        }

    }

}
