package org.apache.rocketmq.streams.serialization;

public interface KeyValueSerializer<KEY, T> {
    byte[] serialize(KEY key, T data) throws Throwable;
}
