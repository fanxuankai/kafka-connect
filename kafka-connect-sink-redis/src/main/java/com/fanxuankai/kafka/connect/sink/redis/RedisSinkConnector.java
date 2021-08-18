package com.fanxuankai.kafka.connect.sink.redis;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author fanxuankai
 */
public class RedisSinkConnector extends SinkConnector {
    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedisSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return IntStream.range(0, maxTasks)
                .mapToObj(value -> props)
                .collect(Collectors.toList());
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return RedisSinkConnectorConfig.config();
    }

    @Override
    public String version() {
        return "0.0.1";
    }
}
