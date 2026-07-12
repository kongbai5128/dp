package com.hmdp.consumer;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Slf4j
@Component
// @KafkaListener(id = "multiGroup", topics = "greeting")
public class SeckillVoucherConsumer {

    @Resource
    private IVoucherOrderService voucherOrderService;

    /**
     * 【秒杀链路消息队列使用】-3
     * Kafka 监听消费秒杀订单消息
     * 指定之前创建的containerFactory
     */
    @KafkaListener(
//            containerFactory = "seckillVoucherOrderKafkaListenerContainerFactory",
            topics = "seckill-voucher-order"
    )
    public void processMessage(String message, Acknowledgment ack) {
        try {
            log.info("收到秒杀订单消息: {}", message);
            Long userId = Long.valueOf(message.split("userId=")[1].split(",")[0]);
            Long voucherId = Long.valueOf(message.split("voucherId=")[1].split(",")[0]);
            Long orderId = Long.valueOf(message.split("id=")[1].split(",")[0]);

            VoucherOrder order = new VoucherOrder();
            order.setId(orderId);
            order.setUserId(userId);
            order.setVoucherId(voucherId);

            voucherOrderService.createVoucherOrder(order);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("消费秒杀消息异常: {}", e.getMessage(), e);
        }
    }
}