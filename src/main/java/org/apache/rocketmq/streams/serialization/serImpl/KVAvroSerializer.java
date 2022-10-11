package org.apache.rocketmq.streams.serialization.serImpl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.rocketmq.streams.serialization.KeyValueSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * how to encode KV
 * <pre>
 * +-----------+---------------+-----------+-------------+
 * | Int(4)    | Int(4)        | key bytes | value bytes |
 * | key length| value length  |           |             |
 * +-----------+---------------+-----------+-------------+
 * </pre>
 */
public class KVAvroSerializer<K, V> implements KeyValueSerializer<K, V> {
    private Schema keySchema;
    private Schema valueSchema;

    @Override
    @SuppressWarnings("unchecked")
    public byte[] serialize(K key, V value) throws Throwable {
        byte[] keyBytes;

        if (key == null) {
            keyBytes = new byte[0];
        } else {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);

                if (keySchema == null) {
                    Class<K> keyClass = (Class<K>) key.getClass();
                    this.keySchema = SpecificData.get().getSchema(keyClass);
                }

                SpecificDatumWriter<K> keyWriter = new SpecificDatumWriter<>(keySchema);

                keyWriter.write(key, encoder);
                encoder.flush();

                keyBytes = out.toByteArray();
            }
        }


        byte[] valueBytes;
        if (value == null) {
            valueBytes = new byte[0];
        } else {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);

                if (valueSchema == null) {
                    Class<V> valueClass = (Class<V>) value.getClass();
                    this.valueSchema = SpecificData.get().getSchema(valueClass);
                }

                SpecificDatumWriter<V> valueWriter = new SpecificDatumWriter<>(valueSchema);

                valueWriter.write(value, encoder);
                encoder.flush();

                valueBytes = out.toByteArray();
            }
        }

        if (keyBytes.length == 0 && valueBytes.length == 0) {
            return null;
        }

        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer(16);
        buf.writeInt(keyBytes.length);
        buf.writeInt(valueBytes.length);
        buf.writeBytes(keyBytes);
        buf.writeBytes(valueBytes);

        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);

        return bytes;
    }
}
