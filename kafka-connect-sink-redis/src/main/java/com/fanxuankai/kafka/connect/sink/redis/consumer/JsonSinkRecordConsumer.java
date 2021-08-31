package com.fanxuankai.kafka.connect.sink.redis.consumer;

import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.List;

/**
 * @author fanxuankai
 */
public class JsonSinkRecordConsumer implements SinkRecordConsumer {
    private final RedisClusterAsyncCommands<String, String> commands;

    public JsonSinkRecordConsumer(RedisClusterAsyncCommands<String, String> commands) {
        this.commands = commands;
    }

    @Override
    public void accept(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord sinkRecord : sinkRecords) {
            if (sinkRecord.value() == null) {
                commands.hdel(sinkRecord.topic(), sinkRecord.key().toString());
            } else {
                Schema valueSchema = sinkRecord.valueSchema();
                Struct value = (Struct) sinkRecord.value();
                List<Field> fields = valueSchema.fields();
                JSONObject jsonObject = new JSONObject();
                for (Field field : fields) {
                    jsonObject.put(field.name(), value.get(field));
                }
                commands.hset(sinkRecord.topic(), sinkRecord.key().toString(), jsonObject.toJSONString());
            }
        }
    }
}
