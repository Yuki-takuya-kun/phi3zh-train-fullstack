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
import phi3zh.common.utils.RedissonClientFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public abstract class Controller {

    protected Properties kafkaConfig;
    protected Config redisConfig;

    protected List<Triple<String, Integer, Short>> kafkaTopics;
    protected List<Pair<String, Boolean>> booleanBucketWithDefaults;
    protected Map<String, Integer> semaphoreWithDefaultValues;

    protected AdminClient kafkaClient;
    protected RedissonClient redissonClient;

    private List<Pair<Future, Triple<String, String, Integer>>> monitorAttrs = new ArrayList<>();

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

    protected void setSemaphoreWithDefaultValues(Map<String, Integer> semaphoreWithDefaultValues){
        this.semaphoreWithDefaultValues = semaphoreWithDefaultValues;
    }

    protected void initializeKafka(){
        kafkaClient = AdminClient.create(kafkaConfig);
        try {
            Set<String> topicNames = kafkaClient.listTopics().names().get();
            kafkaClient.deleteTopics(kafkaTopics.stream().flatMap(elem -> {
                List<String> result = new ArrayList<>();
                String topic = elem.getLeft();
                if (topicNames.contains(topic)){
                    result.add(topic);
                }
                return result.stream();
            }).collect(Collectors.toList())).all().get();
            List<NewTopic> newTopics = kafkaTopics.stream().map(topic ->
                            new NewTopic(topic.getLeft(), topic.getMiddle(), topic.getRight()))
                    .collect(Collectors.toList());
            kafkaClient.createTopics(newTopics).all().get();
        } catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    protected void shutdownKafka(){
        kafkaClient.deleteTopics(kafkaTopics.stream().map(Triple::getLeft).collect(Collectors.toList()));
        kafkaClient.close();
    }

    protected void initializeRedis(){
        redissonClient = RedissonClientFactory.getInstance(redisConfig);
        booleanBucketWithDefaults.stream().forEach(pair -> {
            RBucket<Boolean> booleanRBucket = redissonClient.getBucket(pair.getLeft());
            booleanRBucket.set(pair.getRight());
        });
        semaphoreWithDefaultValues.keySet().forEach(key -> {
            if (redissonClient.getSemaphore(key).isExists()){
                redissonClient.getSemaphore(key).delete();
            }
            RSemaphore semaphore = redissonClient.getSemaphore(key);
            semaphore.trySetPermits(semaphoreWithDefaultValues.get(key));
        });
    }

    protected void shutdownRedis(){
        booleanBucketWithDefaults.stream().forEach(pair -> redissonClient.getBucket(pair.getLeft()).delete());
        semaphoreWithDefaultValues.keySet().forEach(key -> redissonClient.getBucket(key).delete());
        RedissonClientFactory.shutdown();
    }


    protected void addMonitorAttr(Future future, String signalBucket, String monitorSemaphore, Integer iniVal){
        if (signalBucket != null && monitorSemaphore != null && iniVal!=null){
            this.monitorAttrs.add(Pair.of(future, Triple.of(signalBucket, monitorSemaphore, iniVal)));
        } else if (signalBucket == null && monitorSemaphore == null && iniVal == null){
            this.monitorAttrs.add(Pair.of(future, Triple.of(null, null, null)));
        } else {
            throw new RuntimeException("signalBucket, monitorSemaphore and iniVal should bot null");
        }
    }

    protected void runMonitor(){
        boolean allDone = false;
        while (!allDone){
            boolean allDoneCp = true;
            for (Pair<Future, Triple<String, String, Integer>> elem: this.monitorAttrs){
                Triple<String, String, Integer> triple = elem.getRight();
                boolean done = checkDoneTrigger(elem.getLeft(), triple.getLeft(), triple.getMiddle(), triple.getRight());
                allDoneCp = done && allDoneCp;
            }
            allDone = allDoneCp;
            try {
                Thread.sleep(1000);
            } catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    private boolean checkDoneTrigger(Future future, String signalBucket, String monitorSemaphore, Integer iniVal){
        try {
            System.out.println("========");
            System.out.println(signalBucket);
            System.out.println(monitorSemaphore);
            System.out.println(future.isDone());
            if(!future.isDone()){
                return false;
            } else if (monitorSemaphore != null && iniVal != null && signalBucket != null &&
                    redissonClient.getSemaphore(monitorSemaphore).availablePermits() - iniVal == 0){
                RBucket<Boolean> signal = redissonClient.getBucket(signalBucket);
                signal.set(true);
                return true;
            } else {
                return true;
            }
        } catch (Exception e){
            throw new RuntimeException(e);
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
                //bucket.set(true);
                return true;
            } else {
                return false;
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        }

    }

}
