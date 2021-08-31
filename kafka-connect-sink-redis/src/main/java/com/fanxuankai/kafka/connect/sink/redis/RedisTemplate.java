package com.fanxuankai.kafka.connect.sink.redis;

import com.fanxuankai.kafka.connect.sink.redis.config.RedisConnectorConfig;
import com.fanxuankai.kafka.connect.sink.redis.config.RedisSinkConnectorConfig;
import com.fanxuankai.kafka.connect.sink.redis.consumer.JsonSinkRecordConsumer;
import com.fanxuankai.kafka.connect.sink.redis.consumer.SinkRecordConsumer;
import com.fanxuankai.kafka.connect.sink.redis.consumer.SpringSinkRecordConsumer;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.SslOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.kafka.connect.sink.SinkRecord;

import java.time.Duration;
import java.util.Collection;

/**
 * @author fanxuankai
 */
class RedisTemplate {
    private final RedisSinkConnectorConfig config;
    private StatefulRedisClusterConnection<String, String> clusterConnection;
    private StatefulRedisConnection<String, String> connection;
    private RedisClient redisClient;
    private RedisClusterClient redisClusterClient;
    private SinkRecordConsumer sinkRecordConsumer;

    public RedisTemplate(RedisSinkConnectorConfig config) {
        this.config = config;
        final SslOptions sslOptions;
        if (config.sslEnabled) {
            SslOptions.Builder builder = SslOptions.builder();
            switch (config.sslProvider) {
                case JDK:
                    builder.jdkSslProvider();
                    break;
                case OPENSSL:
                    builder.openSslProvider();
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "%s is not a supported value for %s.",
                                    config.sslProvider,
                                    RedisConnectorConfig.SSL_PROVIDER_CONFIG
                            )
                    );
            }
            if (null != config.keystorePath) {
                if (null != config.keystorePassword) {
                    builder.keystore(config.keystorePath, config.keystorePassword.toCharArray());
                } else {
                    builder.keystore(config.keystorePath);
                }
            }
            if (null != config.truststorePath) {
                if (null != config.truststorePassword) {
                    builder.truststore(config.truststorePath, config.keystorePassword);
                } else {
                    builder.truststore(config.truststorePath);
                }
            }
            sslOptions = builder.build();
        } else {
            sslOptions = null;
        }

        final SocketOptions socketOptions = SocketOptions.builder()
                .tcpNoDelay(config.tcpNoDelay)
                .connectTimeout(Duration.ofMillis(config.connectTimeout))
                .keepAlive(config.keepAliveEnabled)
                .build();


        if (RedisConnectorConfig.ClientMode.Cluster == config.clientMode) {
            ClusterClientOptions.Builder clientOptions = ClusterClientOptions.builder()
                    .requestQueueSize(config.requestQueueSize)
                    .autoReconnect(config.autoReconnectEnabled);
            if (config.sslEnabled) {
                clientOptions.sslOptions(sslOptions);
            }
            redisClusterClient = RedisClusterClient.create(config.redisUris());
            redisClusterClient.setOptions(clientOptions.build());
            clusterConnection = redisClusterClient.connect();
        } else if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
            final ClientOptions.Builder clientOptions = ClientOptions.builder()
                    .socketOptions(socketOptions)
                    .requestQueueSize(config.requestQueueSize)
                    .autoReconnect(config.autoReconnectEnabled);
            if (config.sslEnabled) {
                clientOptions.sslOptions(sslOptions);
            }
            redisClient = RedisClient.create(config.redisUris().get(0));
            redisClient.setOptions(clientOptions.build());
            connection = redisClient.connect();
        } else {
            throw new UnsupportedOperationException(
                    String.format("%s is not supported", config.clientMode)
            );
        }

        RedisClusterAsyncCommands<String, String> commands;
        if (RedisConnectorConfig.ClientMode.Cluster == config.clientMode) {
            commands = clusterConnection.async();
        } else {
            commands = connection.async();
        }
        if (config.storageFormat == RedisSinkConnectorConfig.StorageFormat.Json) {
            sinkRecordConsumer = new JsonSinkRecordConsumer(commands, config);
        } else if (config.storageFormat == RedisSinkConnectorConfig.StorageFormat.Spring) {
            sinkRecordConsumer = new SpringSinkRecordConsumer(commands, config);
        }
    }

    public void put(Collection<SinkRecord> records) {
        sinkRecordConsumer.accept(records);
    }

    public void stop() {
        sinkRecordConsumer = null;
        if (RedisConnectorConfig.ClientMode.Cluster == config.clientMode) {
            clusterConnection.close();
            redisClusterClient.shutdown();
        } else if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
            connection.close();
            redisClient.shutdown();
        }
    }
}
