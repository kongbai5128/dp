package com.hmdp.task;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Component
public class OrderAutoCloseTask {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * 库存恢复Kafka topic
     */
    private static final String STOCK_RESTORE_TOPIC = "order-stock-restore";

    @Scheduled(fixedRate = 60 * 1000)
    public void closeExpiredOrders() {
        log.info("开始执行未支付订单自动关闭任务...");
        LocalDateTime expireTime = LocalDateTime.now().minusMinutes(30);
        // 1.查询所有超时的未支付订单
        List<VoucherOrder> expiredOrders = voucherOrderService.list(
                new QueryWrapper<VoucherOrder>()
                        .eq("status", 1)
                        .lt("create_time", expireTime)
        );
        int closedCount = 0;
        for (VoucherOrder order : expiredOrders) {
            // 2.乐观锁：条件更新，只有 status 仍为 1（未支付）时才关闭
            //    如果用户在此期间完成了支付，status 已变为 2，update 不会生效
            boolean success = voucherOrderService.update(
                    new UpdateWrapper<VoucherOrder>()
                            .eq("id", order.getId())
                            .eq("status", 1) // 乐观锁条件：确保仍是未支付状态
                            .set("status", 4)
                            .set("update_time", LocalDateTime.now())
            );
            if (success) {
                closedCount++;
                log.info("订单 {} 已超时自动关闭", order.getId());
                // 3.关闭成功后，发送Kafka消息异步恢复库存
                kafkaTemplate.send(STOCK_RESTORE_TOPIC, String.valueOf(order.getVoucherId()));
                log.info("已发送库存恢复消息，voucherId={}", order.getVoucherId());
            } else {
                log.info("订单 {} 关闭失败，可能已被支付或取消", order.getId());
            }
        }
        log.info("未支付订单自动关闭任务完成，共关闭 {} 个订单", closedCount);
    }
}
