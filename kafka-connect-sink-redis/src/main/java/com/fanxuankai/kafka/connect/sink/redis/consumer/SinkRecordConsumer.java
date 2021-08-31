package com.fanxuankai.kafka.connect.sink.redis.consumer;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * @author fanxuankai
 */
public interface SinkRecordConsumer extends Consumer<Collection<SinkRecord>> {
}
