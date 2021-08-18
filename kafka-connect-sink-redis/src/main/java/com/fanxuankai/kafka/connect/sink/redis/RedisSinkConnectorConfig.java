package com.fanxuankai.kafka.connect.sink.redis;

import org.apache.kafka.common.config.ConfigDef;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class RedisSinkConnectorConfig extends RedisConnectorConfig {

    public final static String OPERATION_TIMEOUT_MS_CONF = "redis.operation.timeout.ms";
    final static String OPERATION_TIMEOUT_MS_DOC = "The amount of time in milliseconds before an" +
            " operation is marked as timed out.";

    public final static String CHARSET_CONF = "redis.charset";
    public final static String CHARSET_DOC = "The character set to use for String key and values.";

    public final long operationTimeoutMs;
    public final Charset charset;

    public RedisSinkConnectorConfig(Map<?, ?> originals) {
        super(config(), originals);
        this.operationTimeoutMs = getLong(OPERATION_TIMEOUT_MS_CONF);
        String charset = getString(CHARSET_CONF);
        this.charset = Charset.forName(charset);
    }

    public static ConfigDef config() {
        return RedisConnectorConfig.config()
                .define(
                        ConfigKeyBuilder.of(DEFAULT_GROUP, OPERATION_TIMEOUT_MS_CONF, ConfigDef.Type.LONG)
                                .documentation(OPERATION_TIMEOUT_MS_DOC)
                                .defaultValue(10000L)
                                .validator(ConfigDef.Range.atLeast(100L))
                                .importance(ConfigDef.Importance.MEDIUM)
                                .build()
                ).define(
                        ConfigKeyBuilder.of(DEFAULT_GROUP, CHARSET_CONF, ConfigDef.Type.STRING)
                                .documentation(CHARSET_DOC)
                                .defaultValue("UTF-8")
                                .validator(new ValidCharset())
                                .recommender(new ConfigDef.Recommender() {
                                    @Override
                                    public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
                                        return new ArrayList<>(Charset.availableCharsets().keySet());
                                    }

                                    @Override
                                    public boolean visible(String name, Map<String, Object> parsedConfig) {
                                        return true;
                                    }
                                })
                                .importance(ConfigDef.Importance.LOW)
                                .build()
                );
    }

}
