package org.apache.rocketmq.streams.serialization;

import org.apache.rocketmq.common.Pair;

public interface KeyValueDeserializer<K, V> {
    default void configure(Object... args) throws Throwable {
    }

    Pair<K, V> deserialize(byte[] total) throws Throwable;
}
