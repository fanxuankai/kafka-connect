package com.fanxuankai.kafka.connect.sink.redis.consumer;

import com.alibaba.fastjson.JSONObject;
import com.fanxuankai.kafka.connect.sink.redis.config.RedisSinkConnectorConfig;
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
public class JsonSinkRecordConsumer extends AbstractSinkRecordConsumer {

    public JsonSinkRecordConsumer(RedisClusterAsyncCommands<String, String> commands, RedisSinkConnectorConfig config) {
        super(commands, config);
    }

    @Override
    public void accept(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord sinkRecord : sinkRecords) {
            if (sinkRecord.value() == null) {
                wait(commands.hdel(sinkRecord.topic(), sinkRecord.key().toString()));
            } else {
                Schema valueSchema = sinkRecord.valueSchema();
                Struct value = (Struct) sinkRecord.value();
                List<Field> fields = valueSchema.fields();
                JSONObject jsonObject = new JSONObject();
                for (Field field : fields) {
                    jsonObject.put(field.name(), value.get(field));
                }
                wait(commands.hset(sinkRecord.topic(), sinkRecord.key().toString(), jsonObject.toJSONString()));
            }
        }
    }
}
