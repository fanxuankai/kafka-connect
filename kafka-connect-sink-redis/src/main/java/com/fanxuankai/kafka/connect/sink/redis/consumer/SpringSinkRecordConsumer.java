package com.fanxuankai.kafka.connect.sink.redis.consumer;

import cn.hutool.core.text.StrPool;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author fanxuankai
 */
public class SpringSinkRecordConsumer implements SinkRecordConsumer {
    private final RedisClusterAsyncCommands<String, String> commands;

    public SpringSinkRecordConsumer(RedisClusterAsyncCommands<String, String> commands) {
        this.commands = commands;
    }

    @Override
    public void accept(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord sinkRecord : sinkRecords) {
            if (sinkRecord.value() == null) {
                commands.srem(sinkRecord.topic(), sinkRecord.key().toString());
                commands.del(sinkRecord.topic() + StrPool.COLON + sinkRecord.key().toString());
            } else {
                Schema valueSchema = sinkRecord.valueSchema();
                Struct value = (Struct) sinkRecord.value();
                List<Field> fields = valueSchema.fields();
                Map<String, String> map = new HashMap<>(fields.size());
                for (Field field : fields) {
                    Object fieldValue = value.get(field);
                    map.put(field.name(), fieldValue == null ? null : fieldValue.toString());
                }
                commands.hmset(sinkRecord.topic() + StrPool.COLON + sinkRecord.key().toString(), map);
                commands.sadd(sinkRecord.topic(), sinkRecord.key().toString());
            }
        }
    }
}
