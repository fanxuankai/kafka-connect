package com.fanxuankai.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

/**
 * @author fanxuankai
 */
public class KeyAppendTopic<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC =
            "Key append topic. "
                    + "<p/>Use the concrete transformation type designed for the record key " +
                    "(<code>" + KeyAppendTopic.class.getName() + "</code>).";

    private static final String SEPARATOR_CONFIG = "separator";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SEPARATOR_CONFIG, ConfigDef.Type.STRING, "_", ConfigDef.Importance.MEDIUM,
                    "Separator for Append topic.");

    private String separator;

    @Override
    public R apply(R record) {
        Object value = record.key();
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
                value == null ? null : value + separator + record.topic(),
                record.valueSchema(), record.value(), record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        separator = config.getString(SEPARATOR_CONFIG);
    }
}
