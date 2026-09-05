package com.damai.core;

import com.damai.config.DelayQueueProperties;
import com.damai.context.DelayQueuePart;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBlockingQueue;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: 极度真实还原大麦网高并发实战项目。 添加 阿星不是程序员 微信，添加时备注 大麦 来获取项目的完整资料 
 * @description: 延迟队列 消费
 * @author: 阿星不是程序员
 **/
@Slf4j
public class DelayConsumerQueue extends DelayBaseQueue{
    
    private final AtomicInteger listenStartThreadCount = new AtomicInteger(1);
    
    private final AtomicInteger executeTaskThreadCount = new AtomicInteger(1);
    
    private final ThreadPoolExecutor listenStartThreadPool;
    
    private final ThreadPoolExecutor executeTaskThreadPool;
    
    private final AtomicBoolean runFlag = new AtomicBoolean(false);
    
    private final ConsumerTask consumerTask;
    /**
     * 当前消费队列对应的逻辑topic，
     * 实际已经包含隔离分片编号。
     *
     * 例如：
     * damai-d_delay_order_cancel_topic-0
     */
    private final String relTopic;

    /**
     * 消费失败后重新投递使用。
     */
    private final DelayProduceQueue retryProduceQueue;

    /**
     * 超过最大重试次数以后保存失败消息。
     */
    private final RBlockingQueue<String> deadLetterQueue;

    private final DelayQueueProperties delayQueueProperties;

    public DelayConsumerQueue(DelayQueuePart delayQueuePart, String relTopic){
        super(delayQueuePart.getDelayQueueBasePart().getRedissonClient(),relTopic);
        this.listenStartThreadPool = new ThreadPoolExecutor(1,1,60,
                TimeUnit.SECONDS,new LinkedBlockingQueue<>(),r -> new Thread(Thread.currentThread().getThreadGroup(), r,
                "listen-start-thread-" + listenStartThreadCount.getAndIncrement()));
        this.executeTaskThreadPool = new ThreadPoolExecutor(
                delayQueuePart.getDelayQueueBasePart().getDelayQueueProperties().getCorePoolSize(),
                delayQueuePart.getDelayQueueBasePart().getDelayQueueProperties().getMaximumPoolSize(),
                delayQueuePart.getDelayQueueBasePart().getDelayQueueProperties().getKeepAliveTime(),
                delayQueuePart.getDelayQueueBasePart().getDelayQueueProperties().getUnit(),
                new LinkedBlockingQueue<>(delayQueuePart.getDelayQueueBasePart().getDelayQueueProperties().getWorkQueueSize()),
                r -> new Thread(Thread.currentThread().getThreadGroup(), r,
                        "delay-queue-consume-thread-" + executeTaskThreadCount.getAndIncrement()));
        this.consumerTask = delayQueuePart.getConsumerTask();
        this.relTopic = relTopic;

        this.delayQueueProperties =
                delayQueuePart
                        .getDelayQueueBasePart()
                        .getDelayQueueProperties();

        /*
         * 重试仍然进入当前分片自己的延迟队列。
         */
        this.retryProduceQueue =
                new DelayProduceQueue(
                        delayQueuePart
                                .getDelayQueueBasePart()
                                .getRedissonClient(),
                        relTopic
                );

        /*
         * 每一个分片拥有自己的dead-letter队列。
         *
         * 例如：
         * damai-d_delay_order_cancel_topic-0.dead-letter
         */
        this.deadLetterQueue =
                delayQueuePart
                        .getDelayQueueBasePart()
                        .getRedissonClient()
                        .getBlockingQueue(
                                relTopic
                                        + delayQueueProperties
                                        .getDeadLetterSuffix()
                        );
    }
    /**
     * 执行一条延迟消息。
     */
    private void consume(String content) {

        DelayRetryMessage retryMessage =
                DelayRetryMessage.decode(content);

        try {

            /*
             * 业务层永远只接收真正的业务payload，
             * 不需要感知框架的retry包装。
             */
            consumerTask.execute(
                    retryMessage.getPayload()
            );

            if (retryMessage.getRetryCount() > 0) {

                log.info(
                        "延迟队列消息重试成功，topic={}，retryCount={}",
                        relTopic,
                        retryMessage.getRetryCount()
                );
            }

        } catch (Exception e) {

            handleConsumeFailure(
                    retryMessage,
                    e
            );
        }
    }
    /**
     * 消费失败后的恢复流程。
     */
    private void handleConsumeFailure(
            DelayRetryMessage retryMessage,
            Exception exception) {

        int currentRetryCount =
                retryMessage.getRetryCount();

        int maxRetryCount =
                delayQueueProperties
                        .getMaxRetryCount();

        /*
         * 还有重试机会。
         */
        if (currentRetryCount < maxRetryCount) {

            int nextRetryCount =
                    currentRetryCount + 1;

            long retryDelay =
                    calculateRetryDelay(
                            currentRetryCount
                    );

            DelayRetryMessage nextMessage =
                    new DelayRetryMessage(
                            retryMessage.getPayload(),
                            nextRetryCount
                    );

            try {

                retryProduceQueue.offer(
                        nextMessage.encode(),
                        retryDelay,
                        delayQueueProperties
                                .getRetryTimeUnit()
                );

                log.warn(
                        "延迟队列消息消费失败，已安排重试，topic={}，retry={}/{}, delay={} {}, error={}",
                        relTopic,
                        nextRetryCount,
                        maxRetryCount,
                        retryDelay,
                        delayQueueProperties
                                .getRetryTimeUnit(),
                        exception.getMessage()
                );

                return;

            } catch (Exception retryException) {

                /*
                 * 连重新放回重试队列都失败，
                 * 直接尝试保存到dead-letter。
                 */
                log.error(
                        "延迟队列消息重新入队失败，topic={}，retryCount={}",
                        relTopic,
                        nextRetryCount,
                        retryException
                );

                moveToDeadLetter(
                        nextMessage,
                        exception
                );

                return;
            }
        }

        /*
         * 已经超过最大重试次数。
         */
        moveToDeadLetter(
                retryMessage,
                exception
        );
    }
    /**
     * 指数退避：
     *
     * retryCount=0 -> 1
     * retryCount=1 -> 2
     * retryCount=2 -> 4
     */
    private long calculateRetryDelay(
            int currentRetryCount) {

        long initialDelay =
                delayQueueProperties
                        .getRetryInitialDelay();

        int safeRetryCount =
                Math.min(
                        currentRetryCount,
                        20
                );

        return initialDelay
                * (1L << safeRetryCount);
    }
    /**
     * 超过最大重试次数后，
     * 将消息保存至dead-letter队列。
     */
    private void moveToDeadLetter(
            DelayRetryMessage retryMessage,
            Exception exception) {

        try {

            deadLetterQueue.put(
                    retryMessage.encode()
            );

            log.error(
                    "延迟队列消息超过最大重试次数，已进入dead-letter，topic={}，deadLetterTopic={}，retryCount={}，error={}",
                    relTopic,
                    relTopic
                            + delayQueueProperties
                            .getDeadLetterSuffix(),
                    retryMessage.getRetryCount(),
                    exception.getMessage()
            );

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            log.error(
                    "延迟队列消息写入dead-letter时线程被中断，topic={}",
                    relTopic,
                    e
            );
        } catch (Exception e) {

            log.error(
                    "延迟队列消息写入dead-letter失败，topic={}",
                    relTopic,
                    e
            );
        }
    }
    public synchronized void listenStart(){
        if (!runFlag.get()) {
            runFlag.set(true);
            listenStartThreadPool.execute(() -> {
                while (!Thread.interrupted()) {
                    try {
                        assert blockingQueue != null;
                        String content =
                                blockingQueue.take();

                        try {

                            executeTaskThreadPool.execute(
                                    () -> consume(content)
                            );

                        } catch (Exception e) {

                            /*
                             * 连消费线程池提交任务都失败时，
                             * 消息也不能直接丢失。
                             */
                            log.error(
                                    "延迟队列消费任务提交失败，topic={}",
                                    relTopic,
                                    e
                            );

                            handleConsumeFailure(
                                    DelayRetryMessage.decode(content),
                                    e
                            );
                        }
                    } catch (InterruptedException e) {
                        destroy(executeTaskThreadPool);
                    } catch (Throwable e) {
                        log.error("blockingQueue take error",e);
                    }
                }
            });
        }
    }
    
    public void destroy(ExecutorService executorService) {
        try {
            if (Objects.nonNull(executorService)) {
                executorService.shutdown();
            }
        } catch (Exception e) {
            log.error("destroy error",e);
        }
    }
}
