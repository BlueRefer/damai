package com.damai.service.delayconsumer;

import com.alibaba.fastjson.JSON;
import com.damai.core.SpringUtil;
import com.damai.util.StringUtil;
import com.damai.core.ConsumerTask;
import com.damai.dto.DelayOrderCancelDto;
import com.damai.dto.OrderCancelDto;
import com.damai.service.OrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.damai.enums.BaseCode;
import com.damai.exception.DaMaiFrameException;
import static com.damai.service.constant.OrderConstant.DELAY_ORDER_CANCEL_TOPIC;

/**
 * @program: 极度真实还原大麦网高并发实战项目。 添加 阿星不是程序员 微信，添加时备注 大麦 来获取项目的完整资料 
 * @description: 延迟订单取消
 * @author: 阿星不是程序员
 **/
@Slf4j
@Component
public class DelayOrderCancelConsumer implements ConsumerTask {
    
    @Autowired
    private OrderService orderService;
    
    /**
     * ⚠️注意！在升级的大麦pro版本中进行了优化，解决了延迟队列数据丢失、消费失败等问题，提高了消息的可靠性
     * ✅大麦pro升级功能的详细介绍，请看 <a href="https://javaup.chat/damai/damai-pro/release-intro">...</a>
     * ✨如何获取大麦pro项目？请看 <a href="https://articles.zsxq.com/id_m4d7ni4zwkbq.html">...</a>
     **/
    @Override
    public void execute(String content) {
        log.info("延迟订单取消消息进行消费 content : {}", content);
        if (StringUtil.isEmpty(content)) {
            log.error("延迟队列消息不存在");
            return;
        }
        DelayOrderCancelDto delayOrderCancelDto = JSON.parseObject(content, DelayOrderCancelDto.class);
        
        //取消订单
        OrderCancelDto orderCancelDto = new OrderCancelDto();
        orderCancelDto.setOrderNumber(delayOrderCancelDto.getOrderNumber());
        try {

            boolean cancel =
                    orderService.cancel(orderCancelDto);

            if (cancel) {

                log.info(
                        "延迟订单取消成功，orderNumber={}",
                        orderCancelDto.getOrderNumber()
                );

            } else {

                log.error(
                        "延迟订单取消失败，orderNumber={}",
                        orderCancelDto.getOrderNumber()
                );
            }

        } catch (DaMaiFrameException e) {

            /*
             * 延迟消息可能重复投递，
             * 或订单已经被其他流程取消。
             *
             * 已取消属于目标状态，
             * 因此按照幂等成功处理。
             */
            if (BaseCode.ORDER_CANCEL
                    .getCode()
                    .equals(e.getCode())) {

                log.info(
                        "延迟取消消息重复消费，订单已经取消，按幂等成功处理，orderNumber={}",
                        orderCancelDto.getOrderNumber()
                );

                return;
            }

            /*
             * 用户已经完成支付后，
             * 延迟取消任务仍然可能到达。
             *
             * 已支付订单不能再执行取消，
             * 这里属于正常业务跳过，而不是消费失败。
             */
            if (BaseCode.ORDER_PAY
                    .getCode()
                    .equals(e.getCode())) {

                log.info(
                        "延迟取消消息到达时订单已支付，跳过取消，orderNumber={}",
                        orderCancelDto.getOrderNumber()
                );

                return;
            }

            /*
             * 已退款也是终态，
             * 无需再次执行取消。
             */
            if (BaseCode.ORDER_REFUND
                    .getCode()
                    .equals(e.getCode())) {

                log.info(
                        "延迟取消消息到达时订单已退款，跳过取消，orderNumber={}",
                        orderCancelDto.getOrderNumber()
                );

                return;
            }

            /*
             * 订单不存在、数据库异常等其他问题
             * 不能吞掉，继续向上抛。
             */
            throw e;
        }
    }
    
    @Override
    public String topic() {
        return SpringUtil.getPrefixDistinctionName() + "-" + DELAY_ORDER_CANCEL_TOPIC;
    }
}
