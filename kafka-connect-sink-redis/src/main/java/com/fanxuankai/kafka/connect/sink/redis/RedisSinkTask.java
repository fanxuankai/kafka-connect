package com.fanxuankai.kafka.connect.sink.redis;

import com.fanxuankai.kafka.connect.sink.redis.config.RedisSinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

/**
 * @author fanxuankai
 */
public class RedisSinkTask extends SinkTask {
    private RedisTemplate redisTemplate;

    @Override
    public String version() {
        return "0.0.1";
    }

    @Override
    public void start(Map<String, String> props) {
        redisTemplate = new RedisTemplate(new RedisSinkConnectorConfig(props));
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        redisTemplate.put(records);
    }

    @Override
    public void stop() {
        redisTemplate.stop();
    }
}
