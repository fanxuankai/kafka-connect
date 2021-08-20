package com.fanxuankai.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.RegexValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

/**
 * @author fanxuankai
 */
public abstract class RegexFieldValue<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegexFieldValue.class);
    public static final String OVERVIEW_DOC = "Update the record field value using the configured regular expression " +
            "and replacement string."
            + "<p/>Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. "
            + "If the pattern matches the input field value, <code>java.util.regex.Matcher#replaceFirst()</code> is " +
            "used with the replacement string to obtain the new field value.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                    "Field name to Replacement.")
            .define(ConfigName.REGEX, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new RegexValidator(),
                    ConfigDef.Importance.HIGH,
                    "Regular expression to use for matching.")
            .define(ConfigName.REPLACEMENT, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH, "Replacement string.");

    private static final String PURPOSE = "field replacement";

    private static final Pattern TOPIC = Pattern.compile("${topic}", Pattern.LITERAL);

    private interface ConfigName {
        String FIELD = "field";
        String REGEX = "regex";
        String REPLACEMENT = "replacement";
    }

    private String fieldName;
    private Pattern regex;
    private String replacement;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.FIELD);
        regex = Pattern.compile(config.getString(ConfigName.REGEX));
        replacement = config.getString(ConfigName.REPLACEMENT);
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        Object value = operatingValue(record);
        // 既不是 map 也不是 struct 的情况下，字段值就是 value
        Object fieldValue = value;
        if (isMapOrStruct(value)) {
            if (schema == null) {
                final Map<String, Object> map = requireMapOrNull(value, PURPOSE);
                if (map != null) {
                    fieldValue = map.get(fieldName);
                }
            } else {
                final Struct struct = requireStructOrNull(value, PURPOSE);
                Field field = schema.field(fieldName);
                if (field == null) {
                    throw new IllegalArgumentException("Unknown field: " + fieldName);
                }
                fieldValue = struct.get(fieldName);
            }
        }
        Class<?> fieldClass = fieldValue.getClass();
        Object updatedValue = fieldValue;
        if (value != null) {
            String replaceTopicReplacement = TOPIC.matcher(replacement)
                    .replaceAll(Matcher.quoteReplacement(record.topic()));
            Matcher matcher = regex.matcher(fieldValue.toString());
            if (matcher.matches()) {
                fieldValue = matcher.replaceFirst(replaceTopicReplacement);
                try {
                    // 正则替换需要转回原类型
                    updatedValue = fieldClass.cast(fieldValue);
                } catch (Exception e) {
                    LOGGER.info(e.getMessage());
                }
            }
        }
        return newRecord(record, schema, updatedValue);
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends RegexFieldValue<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue,
                    record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends RegexFieldValue<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    updatedSchema, updatedValue, record.timestamp());
        }
    }

    private static boolean isMapOrStruct(Object value) {
        return value instanceof Map || value instanceof Struct;
    }
}
