package com.hmdp.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 缓存删除补偿重试消费者
 * 当更新数据库后删除缓存失败时，通过Kafka异步重试删除
 */
@Slf4j
@Component
public class CacheDeleteRetryConsumer {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @KafkaListener(topics = "cache-delete-retry")
    public void processMessage(String cacheKey, Acknowledgment ack) {
        try {
            log.info("收到缓存删除重试消息: key={}", cacheKey);
            // 重试删除缓存
            stringRedisTemplate.delete(cacheKey);
            log.info("缓存删除重试成功: key={}", cacheKey);
            // 删除成功，手动ACK确认
            ack.acknowledge();
        } catch (Exception e) {
            log.error("缓存删除重试仍然失败: key={}，等待下次重试或TTL兜底过期", cacheKey, e);
            // 不ACK，Kafka会重新投递消息进行重试
        }
    }
}
