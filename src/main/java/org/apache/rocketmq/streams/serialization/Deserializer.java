package org.apache.rocketmq.streams.serialization;

import java.io.Closeable;

public interface Deserializer<T> extends Closeable {
    T deserialize(byte[] data) throws Throwable;

    @Override
    default void close() {
        // intentionally left blank
    }
}
