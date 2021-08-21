package com.fanxuankai.kafka.connect.sink.redis;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.List;
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
        records.forEach(sinkRecord -> {
            if (sinkRecord.value() == null) {
                redisTemplate.hDel(sinkRecord.topic(), sinkRecord.key().toString());
                return;
            }
            Schema valueSchema = sinkRecord.valueSchema();
            Struct value = (Struct) sinkRecord.value();
            List<Field> fields = valueSchema.fields();
            JSONObject jsonObject = new JSONObject();
            for (Field field : fields) {
                jsonObject.put(field.name(), value.get(field));
            }
            redisTemplate.hSet(sinkRecord.topic(),
                    sinkRecord.key().toString(),
                    jsonObject.toJSONString());
        });
    }

    @Override
    public void stop() {
        redisTemplate.stop();
    }
}
