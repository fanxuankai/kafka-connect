package com.fanxuankai.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

/**
 * @author fanxuankai
 */
public abstract class ExtractField<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC =
            "Extract the specified fields from a Struct when schema present, or a Map in the case of schemaless data. "
                    + "Any null values are passed through unmodified."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
                    + "or value (<code>" + Value.class.getName() + "</code>).";
    private static final String FIELDS_CONFIG = "fields";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
                        @SuppressWarnings("unchecked")
                        @Override
                        public void ensureValid(String name, Object valueObject) {
                            List<String> value = (List<String>) valueObject;
                            if (value == null || value.isEmpty()) {
                                throw new ConfigException("Must specify at least one field to extract.");
                            }
                            parseFieldTypes(value);
                        }

                        @Override
                        public String toString() {
                            return "list of pairs, e.g. <code>foo:bar,abc:xyz</code>";
                        }
                    },
                    ConfigDef.Importance.HIGH, "Fields name to extract.");

    private static final String PURPOSE = "fields extraction";
    private static final String WHOLE_VALUE_FIELD = null;
    private Map<String, String> fieldNameByTopic;
    private String wholeValueExtractField;
    private static final String REGEX = ":";

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldNameByTopic = parseFieldTypes(config.getList(FIELDS_CONFIG));
        wholeValueExtractField = fieldNameByTopic.get(WHOLE_VALUE_FIELD);
    }

    @Override
    public R apply(R record) {
        String fieldName;
        if (wholeValueExtractField != null) {
            fieldName = wholeValueExtractField;
        } else {
            fieldName = fieldNameByTopic.get(record.topic());
        }
        final Schema schema = operatingSchema(record);
        if (schema == null) {
            final Map<String, Object> value = requireMapOrNull(operatingValue(record), PURPOSE);
            return newRecord(record, null, value == null ? null : value.get(fieldName));
        } else {
            final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
            Field field = schema.field(fieldName);

            if (field == null) {
                throw new IllegalArgumentException("Unknown field: " + fieldName);
            }

            return newRecord(record, field.schema(), value == null ? null : value.get(fieldName));
        }
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

    public static class Key<R extends ConnectRecord<R>> extends ExtractField<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends ExtractField<R> {
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

    private static Map<String, String> parseFieldTypes(List<String> keys) {
        final Map<String, String> m = new HashMap<>();
        boolean isWholeValueCast = false;
        for (String key : keys) {
            final String[] parts = key.split(REGEX);
            if (parts.length > 2) {
                throw new ConfigException(ExtractField.FIELDS_CONFIG, keys, "Invalid keys: " + key);
            }
            if (parts.length == 1) {
                m.put(WHOLE_VALUE_FIELD, parts[0].trim());
                isWholeValueCast = true;
            } else {
                m.put(parts[0].trim(), parts[1].trim());
            }
        }
        if (isWholeValueCast && keys.size() > 1) {
            throw new ConfigException("Cast transformations that specify a type to cast the entire value to "
                    + "may ony specify a single cast in their spec");
        }
        return m;
    }

}
