package com.damai.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

    /**
     * Kafka消费可靠性配置。
     *
     * 创建订单消息消费失败时：
     * 1. 按固定间隔进行有限次数重试；
     * 2. 超过最大重试次数后写入DLT；
     * 3. 避免异常消息永久阻塞正常订单消息。
     */
    @Slf4j
    @Configuration
    public class KafkaConsumerReliabilityConfig {

        /**
         * 每次重试间隔：1秒。
         */
        private static final long RETRY_INTERVAL_MS = 1000L;

        /**
         * 初始消费失败后额外重试2次。
         * <p>
         * 即：
         * 初始消费 + 2次重试 = 最多3次处理机会。
         */
        private static final long MAX_RETRY_ATTEMPTS = 2L;

        @Bean
        public ConcurrentKafkaListenerContainerFactory<Object, Object>
        kafkaListenerContainerFactory(
                ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
                ConsumerFactory<Object, Object> consumerFactory,
                DefaultErrorHandler kafkaErrorHandler) {

            ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();

            /*
             * 保留 Spring Boot 根据 application.yml
             * 自动配置的 Kafka Consumer 参数。
             */
            configurer.configure(
                    factory,
                    consumerFactory
            );

            /*
             * 使用我们定义的：
             * 重试 + BackOff + DLT 错误处理器。
             */
            factory.setCommonErrorHandler(
                    kafkaErrorHandler
            );

            /*
             * 开启 deliveryAttempt Header。
             *
             * 第一次消费：1
             * 第一次重试：2
             * 第二次重试：3
             */
            factory.getContainerProperties()
                    .setDeliveryAttemptHeader(true);

            return factory;
        }
        @Bean
        public DefaultErrorHandler kafkaErrorHandler(
                KafkaTemplate<String, String> kafkaTemplate) {

            /*
             * 最终仍失败的消息发送到：
             *
             * 原topic.DLT
             *
             * 例如：
             * damai-create_order
             *        ↓
             * damai-create_order.DLT
             *
             * 同时保持原来的partition。
             */
            DeadLetterPublishingRecoverer recoverer =
                    new DeadLetterPublishingRecoverer(
                            kafkaTemplate,
                            (record, exception) ->
                                    new TopicPartition(
                                            record.topic() + ".DLT",
                                            record.partition()
                                    )
                    );

            /*
             * 如果发送DLT本身失败，
             * 不允许把原消息误认为已经成功恢复。
             */
            recoverer.setFailIfSendResultIsError(true);

            FixedBackOff fixedBackOff =
                    new FixedBackOff(
                            RETRY_INTERVAL_MS,
                            MAX_RETRY_ATTEMPTS
                    );

            DefaultErrorHandler errorHandler =
                    new DefaultErrorHandler(
                            recoverer,
                            fixedBackOff
                    );

            /*
             * 记录每一次失败，
             * 后面的实验可以直接从OrderApplication控制台观察。
             */
            errorHandler.setRetryListeners(
                    new RetryListener() {

                        @Override
                        public void failedDelivery(
                                ConsumerRecord<?, ?> record,
                                Exception exception,
                                int deliveryAttempt) {

                            log.warn(
                                    "Kafka消息消费失败，第{}次投递，topic={}，partition={}，offset={}，error={}",
                                    deliveryAttempt,
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    exception.getMessage()
                            );
                        }

                        @Override
                        public void recovered(
                                ConsumerRecord<?, ?> record,
                                Exception exception) {

                            log.error(
                                    "Kafka消息超过最大重试次数，已进入DLT，topic={}，partition={}，offset={}，dltTopic={}",
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    record.topic() + ".DLT"
                            );
                        }

                        @Override
                        public void recoveryFailed(
                                ConsumerRecord<?, ?> record,
                                Exception original,
                                Exception failure) {

                            log.error(
                                    "Kafka消息写入DLT失败，topic={}，partition={}，offset={}",
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    failure
                            );
                        }
                    }
            );

            return errorHandler;
        }
    }