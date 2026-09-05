package com.damai.service.kafka;

import com.damai.enums.BaseCode;
import com.damai.exception.DaMaiFrameException;
import com.alibaba.fastjson.JSON;
import com.damai.dto.OrderCreateDto;
import com.damai.dto.OrderTicketUserCreateDto;
import com.damai.enums.OrderStatus;
import com.damai.service.OrderService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import static com.damai.constant.Constant.SPRING_INJECT_PREFIX_DISTINCTION_NAME;

/**
 * @program: 极度真实还原大麦网高并发实战项目。 添加 阿星不是程序员 微信，添加时备注 大麦 来获取项目的完整资料 
 * @description: kafka 创建订单 消费
 * @author: 阿星不是程序员
 **/
@Slf4j
@AllArgsConstructor
@Component
public class CreateOrderConsumer {
    
    @Autowired
    private OrderService orderService;
    
    public static Long MESSAGE_DELAY_TIME = 5000L;
    
    /**
     * ⚠️注意！在升级的大麦pro版本中进行了优化，加入了消息延迟的废单统计、补偿机制、消息对账、实时监控等功能
     * ✅大麦pro升级功能的详细介绍，请看 <a href="https://javaup.chat/damai/damai-pro/release-intro">...</a>
     * ✨如何获取大麦pro项目？请看 <a href="https://articles.zsxq.com/id_m4d7ni4zwkbq.html">...</a>
     **/
    @KafkaListener(topics = {SPRING_INJECT_PREFIX_DISTINCTION_NAME+"-"+"${spring.kafka.topic:create_order}"})
    public void consumerOrderMessage(
            ConsumerRecord<String, String> consumerRecord,
            @Header(
                    name = KafkaHeaders.DELIVERY_ATTEMPT,
                    required = false
            )
            Integer deliveryAttempt) {

        String value = consumerRecord.value();

        if (value == null) {
            log.warn(
                    "收到空Kafka消息，topic={}，partition={}，offset={}",
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset()
            );
            return;
        }
        int currentDeliveryAttempt =
                deliveryAttempt == null ? 1 : deliveryAttempt;

        try {
            OrderCreateDto orderCreateDto =
                    JSON.parseObject(
                            value,
                            OrderCreateDto.class
                    );



            long createOrderTimeTimestamp =
                    orderCreateDto
                            .getCreateOrderTime()
                            .getTime();

            long currentTimeTimestamp =
                    System.currentTimeMillis();

            long delayTime =
                    currentTimeTimestamp
                            - createOrderTimeTimestamp;

            log.info(
                    "消费创建订单消息，订单号={}，partition={}，offset={}，延迟={}ms，deliveryAttempt={}",
                    orderCreateDto.getOrderNumber(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    delayTime,
                    currentDeliveryAttempt
            );

            /*
             * 消息延迟超过阈值，
             * 沿用项目原来的库存/座位补偿逻辑。
             */
            if (currentDeliveryAttempt == 1
                    && delayTime > MESSAGE_DELAY_TIME) {
                log.warn(
                        "创建订单消息延迟超过{}ms，执行补偿，订单号={}",
                        MESSAGE_DELAY_TIME,
                        orderCreateDto.getOrderNumber()
                );

                Map<Long, List<OrderTicketUserCreateDto>>
                        orderTicketUserSeatList =
                        orderCreateDto
                                .getOrderTicketUserCreateDtoList()
                                .stream()
                                .collect(
                                        Collectors.groupingBy(
                                                OrderTicketUserCreateDto
                                                        ::getTicketCategoryId
                                        )
                                );

                Map<Long, List<Long>> seatMap =
                        new HashMap<>(
                                orderTicketUserSeatList.size()
                        );

                orderTicketUserSeatList.forEach(
                        (ticketCategoryId, ticketUserList) ->
                                seatMap.put(
                                        ticketCategoryId,
                                        ticketUserList
                                                .stream()
                                                .map(
                                                        OrderTicketUserCreateDto
                                                                ::getSeatId
                                                )
                                                .collect(
                                                        Collectors.toList()
                                                )
                                )
                );

                orderService.updateProgramRelatedDataMq(
                        orderCreateDto.getProgramId(),
                        seatMap,
                        OrderStatus.CANCEL
                );

                /*
                 * 补偿成功以后方法正常返回，
                 * ack-mode=record 会提交 offset。
                 */
                return;
            }
            /*
             * 如果第一次到达是及时的，
             * 只是因为Kafka重试导致消息年龄超过5秒，
             * 不能再把它当成延迟废单。
             */
            if (currentDeliveryAttempt > 1
                    && delayTime > MESSAGE_DELAY_TIME) {

                log.info(
                        "Kafka重试消息已超过延迟阈值，但首次投递及时，跳过延迟判废，订单号={}，deliveryAttempt={}，delay={}ms",
                        orderCreateDto.getOrderNumber(),
                        currentDeliveryAttempt,
                        delayTime
                );
            }
            String orderNumber =
                    orderService.createMq(
                            orderCreateDto
                    );


            log.info(
                    "创建订单Kafka消息处理成功，订单号={}，partition={}，offset={}",
                    orderNumber,
                    consumerRecord.partition(),
                    consumerRecord.offset()
            );

        } catch (DaMaiFrameException e) {

            /*
             * Kafka 可能重复投递已经成功处理过的消息。
             *
             * create() 本身会检查 order_number 是否已经存在。
             * 如果已经存在，说明这条业务消息实际上已经完成，
             * 不应该无限重试。
             */
            if (BaseCode.ORDER_EXIST
                    .getCode()
                    .equals(e.getCode())) {

                log.warn(
                        "检测到重复订单消息，按幂等成功处理，topic={}，partition={}，offset={}",
                        consumerRecord.topic(),
                        consumerRecord.partition(),
                        consumerRecord.offset()
                );

                return;
            }

            /*
             * 其他业务异常不能吞掉。
             */
            log.error(
                    "处理创建订单消息失败，业务异常，topic={}，partition={}，offset={}，code={}，message={}",
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    e.getCode(),
                    e.getMessage(),
                    e
            );

            throw e;

        } catch (Exception e) {

            /*
             * 系统异常同样继续向 Kafka Listener 容器抛出。
             * 这样消息不会因为 catch 后正常返回而被错误认为成功。
             */
            log.error(
                    "处理创建订单消息失败，系统异常，topic={}，partition={}，offset={}",
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    e
            );

            throw e;
        }
    }
}
