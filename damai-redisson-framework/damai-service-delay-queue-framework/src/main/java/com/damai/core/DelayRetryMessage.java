package com.damai.core;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * 延迟队列内部重试消息。
 *
 * 第一次业务消息仍保持原有字符串格式。
 * 只有发生消费失败以后，框架才包装成该内部格式，
 * 从而兼容Redis中已经存在的历史消息。
 */
public class DelayRetryMessage {

    /**
     * 内部消息格式标识。
     */
    private static final String PREFIX =
            "__DAMAI_DELAY_RETRY_V1__|";

    private final String payload;

    private final int retryCount;

    public DelayRetryMessage(
            String payload,
            int retryCount) {

        this.payload = payload;
        this.retryCount = retryCount;
    }

    public String getPayload() {
        return payload;
    }

    public int getRetryCount() {
        return retryCount;
    }

    /**
     * 将重试消息编码为Redis可保存的字符串。
     */
    public String encode() {

        String encodedPayload =
                Base64.getEncoder()
                        .encodeToString(
                                payload.getBytes(
                                        StandardCharsets.UTF_8
                                )
                        );

        return PREFIX
                + retryCount
                + "|"
                + encodedPayload;
    }

    /**
     * 解析消息。
     *
     * 如果不是框架内部重试消息，
     * 就按照原始业务消息处理，retryCount=0。
     */
    public static DelayRetryMessage decode(
            String content) {

        if (content == null
                || !content.startsWith(PREFIX)) {

            return new DelayRetryMessage(
                    content,
                    0
            );
        }

        try {

            int retryCountSeparator =
                    content.indexOf(
                            "|",
                            PREFIX.length()
                    );

            if (retryCountSeparator < 0) {

                return new DelayRetryMessage(
                        content,
                        0
                );
            }

            int retryCount =
                    Integer.parseInt(
                            content.substring(
                                    PREFIX.length(),
                                    retryCountSeparator
                            )
                    );

            String encodedPayload =
                    content.substring(
                            retryCountSeparator + 1
                    );

            String payload =
                    new String(
                            Base64.getDecoder()
                                    .decode(
                                            encodedPayload
                                    ),
                            StandardCharsets.UTF_8
                    );

            return new DelayRetryMessage(
                    payload,
                    retryCount
            );

        } catch (Exception e) {

            /*
             * 为兼容旧消息和异常格式，
             * 解析失败时仍按普通业务消息处理。
             */
            return new DelayRetryMessage(
                    content,
                    0
            );
        }
    }
}
