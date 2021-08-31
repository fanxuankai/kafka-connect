package com.fanxuankai.kafka.connect.sink.redis.consumer;

import com.fanxuankai.kafka.connect.sink.redis.config.RedisSinkConnectorConfig;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.kafka.connect.errors.RetriableException;

import java.util.concurrent.TimeUnit;

/**
 * @author fanxuankai
 */
public abstract class AbstractSinkRecordConsumer implements SinkRecordConsumer {
    protected final RedisClusterAsyncCommands<String, String> commands;
    private final RedisSinkConnectorConfig config;

    public AbstractSinkRecordConsumer(RedisClusterAsyncCommands<String, String> commands,
                                      RedisSinkConnectorConfig config) {
        this.commands = commands;
        this.config = config;
    }

    /**
     * 等待命令响应
     *
     * @param future /
     * @param config /
     */
    protected void wait(RedisFuture<?> future) {
        try {
            if (!future.await(config.operationTimeoutMs, TimeUnit.MILLISECONDS)) {
                future.cancel(true);
                throw new RetriableException(
                        String.format("Timeout after %s ms while waiting for operation to complete.",
                                this.config.operationTimeoutMs)
                );
            }
        } catch (InterruptedException e) {
            throw new RetriableException(e);
        }
    }
}
