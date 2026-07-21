package com.hmdp.consumer;

import com.hmdp.service.ISeckillVoucherService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 库存恢复消费者
 * 当订单超时关闭后，异步恢复秒杀券库存（DB + Redis）
 * 若恢复失败，不ACK，Kafka会重新投递实现重试补偿
 */
@Slf4j
@Component
public class StockRestoreConsumer {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final String STOCK_KEY_PREFIX = "seckill:stock:";

    /**
     * 监听库存恢复消息，恢复DB库存和Redis库存
     */
    @KafkaListener(topics = "order-stock-restore")
    public void processMessage(String voucherIdStr, Acknowledgment ack) {
        try {
            Long voucherId = Long.valueOf(voucherIdStr);
            log.info("收到库存恢复消息: voucherId={}", voucherId);

            // 1.恢复DB库存：stock = stock + 1
            boolean dbSuccess = seckillVoucherService.update()
                    .setSql("stock = stock + 1")
                    .eq("voucher_id", voucherId)
                    .update();

            if (!dbSuccess) {
                log.error("DB库存恢复失败: voucherId={}", voucherId);
                return;
            }

            // 2.恢复Redis库存：INCR +1
            String stockKey = STOCK_KEY_PREFIX + voucherId;
            if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(stockKey))) {
                stringRedisTemplate.opsForValue().increment(stockKey);
            }

            log.info("库存恢复成功: voucherId={}", voucherId);
            // 恢复成功，手动ACK
            ack.acknowledge();
        } catch (Exception e) {
            log.error("库存恢复异常，等待重试: {}", e.getMessage(), e);
            // 不ACK，Kafka会重新投递
        }
    }
}
