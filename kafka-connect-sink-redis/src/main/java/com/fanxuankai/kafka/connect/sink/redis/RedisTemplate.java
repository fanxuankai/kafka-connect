package com.fanxuankai.kafka.connect.sink.redis;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.SslOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;

import java.time.Duration;

/**
 * @author fanxuankai
 */
class RedisTemplate {
    private final RedisSinkConnectorConfig config;
    private StatefulRedisClusterConnection<String, String> clusterConnection;
    private StatefulRedisConnection<String, String> connection;
    private RedisClient redisClient;
    private RedisClusterClient redisClusterClient;

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
    }

    public void stop() {
        if (RedisConnectorConfig.ClientMode.Cluster == config.clientMode) {
            clusterConnection.close();
            redisClusterClient.shutdown();
        } else if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
            connection.close();
            redisClient.shutdown();
        }
    }

    public void hSet(String key, String field, String value) {
        if (RedisConnectorConfig.ClientMode.Cluster == config.clientMode) {
            RedisAdvancedClusterAsyncCommands<String, String> asyncCommands = clusterConnection.async();
            asyncCommands.hset(key, field, value);
        } else if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
            RedisAsyncCommands<String, String> asyncCommands = connection.async();
            asyncCommands.hset(key, field, value);
        }
    }

    public void hDel(String key, String... fields) {
        if (RedisConnectorConfig.ClientMode.Cluster == config.clientMode) {
            RedisAdvancedClusterAsyncCommands<String, String> asyncCommands = clusterConnection.async();
            asyncCommands.hdel(key, fields);
        } else if (RedisConnectorConfig.ClientMode.Standalone == config.clientMode) {
            RedisAsyncCommands<String, String> asyncCommands = connection.async();
            asyncCommands.hdel(key, fields);
        }
    }
}
