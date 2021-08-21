package com.fanxuankai.kafka.connect.transforms.predicates;

import com.fanxuankai.kafka.connect.transforms.util.TypeConverter;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author fanxuankai
 */
public abstract class RecordPredicate<R extends ConnectRecord<R>> implements Predicate<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordPredicate.class);
    public static final String OVERVIEW_DOC = "Record predicates are based on JSON path expressions.";
    private static final String CONDITION_CONFIG = "condition";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONDITION_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.NonEmptyString(), ConfigDef.Importance.MEDIUM,
                    "The condition.");


    private JsonPath filterConditionPath;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public boolean test(R record) {
        if (operatingValue(record) == null) {
            return true;
        }
        Schema schema = operatingSchema(record);
        Object data = (schema == null) ? Requirements.requireMap(operatingValue(record), "test record without " +
                "schema") : TypeConverter.convertObject(
                Requirements.requireStruct(operatingValue(record), "test record with schema"), ((Struct)
                        operatingValue(record)).schema());
        List<?> filtered = this.filterConditionPath.read(data,
                Configuration.defaultConfiguration().addOptions(Option.ALWAYS_RETURN_LIST));
        return filtered.size() > 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig simpleConfig = new SimpleConfig(config(), configs);
        this.filterConditionPath = JsonPath.compile(simpleConfig.getString(CONDITION_CONFIG));
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends RecordPredicate<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends RecordPredicate<R> {
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
}
