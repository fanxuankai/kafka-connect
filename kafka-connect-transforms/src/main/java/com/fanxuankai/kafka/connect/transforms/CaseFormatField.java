package com.fanxuankai.kafka.connect.transforms;

import com.google.common.base.CaseFormat;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * 字段名转换：例如大写下划线转小写驼峰
 *
 * @author fanxuankai
 */
public abstract class CaseFormatField<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CaseFormatField.class);
    public static final String OVERVIEW_DOC = "Case format field used <code>com.google.common.base.CaseFormat</code>.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FROM, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH, "From format.")
            .define(ConfigName.TO, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH, "To format.");

    private static final String PURPOSE = "field case format";

    private interface ConfigName {
        String FROM = "from";
        String TO = "to";
    }

    private String from;
    private String to;
    private Cache<Schema, Schema> schemaUpdateCache;
    private Map<String, String> reverseCase;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        from = config.getString(ConfigName.FROM);
        to = config.getString(ConfigName.TO);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
        reverseCase = new HashMap<>(16);
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
        reverseCase = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends CaseFormatField<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends CaseFormatField<R> {
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

    private R applySchemaless(R record) {
        Object operatingValue = operatingValue(record);
        if (!(operatingValue instanceof Map)) {
            return record;
        }
        final Map<String, Object> value = requireMap(operatingValue, PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value.size());
        for (Map.Entry<String, Object> e : value.entrySet()) {
            final String fieldName = e.getKey();
            updatedValue.put(caseFormat(fieldName), e.getValue());
        }
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        Object operatingValue = operatingValue(record);
        if (!(operatingValue instanceof Struct)) {
            return record;
        }
        final Struct value = requireStruct(operatingValue, PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : updatedSchema.fields()) {
            final Object fieldValue = value.get(reverseCase.get(field.name()));
            updatedValue.put(field.name(), fieldValue);
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private String caseFormat(String fieldName) {
        return CaseFormat.valueOf(from).to(CaseFormat.valueOf(to), fieldName);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            String caseFieldName = caseFormat(field.name());
            builder.field(caseFieldName, field.schema());
            reverseCase.put(caseFieldName, field.name());
        }
        return builder.build();
    }
}
