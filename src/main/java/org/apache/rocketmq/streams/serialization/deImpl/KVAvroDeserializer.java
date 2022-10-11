package org.apache.rocketmq.streams.serialization.deImpl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.streams.serialization.KeyValueDeserializer;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

/**
 * how to decode KV
 * <pre>
 * +-----------+---------------+-----------+-------------+
 * | Int(4)    | Int(4)        | key bytes | value bytes |
 * | key length| value length  |           |             |
 * +-----------+---------------+-----------+--------- ---+
 * </pre>
 */
public class KVAvroDeserializer<K, V> implements KeyValueDeserializer<K, V> {
    private DecoderFactory factory;
    private DatumReader<K> keyDatumReader;
    private DatumReader<V> valueDatumReader;



    public KVAvroDeserializer() {
        factory = DecoderFactory.get();
        keyDatumReader = new SpecificDatumReader<>();
        valueDatumReader = new SpecificDatumReader<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Object... args) throws Throwable {
        String keyClassName = (String) args[0];
        if (!StringUtils.isEmpty(keyClassName)) {
            Class<K> keyClass = (Class<K>) Class.forName(keyClassName);
            Schema keySchema = SpecificData.get().getSchema(keyClass);
            keyDatumReader.setSchema(keySchema);
        }

        String valueClassName = (String) args[1];
        if (!StringUtils.isEmpty(valueClassName)) {
            Class<V> valueClass = (Class<V>) Class.forName(valueClassName);
            Schema valueSchema = SpecificData.get().getSchema(valueClass);
            valueDatumReader.setSchema(valueSchema);
        }
    }

    @Override
    public Pair<K, V> deserialize(byte[] total) throws Throwable {
        ByteBuf byteBuf = Unpooled.copiedBuffer(total);

        int keyLength = byteBuf.readInt();
        int valueLength = byteBuf.readInt();
        ByteBuf keyByteBuf = byteBuf.readBytes(keyLength);
        ByteBuf valueByteBuf = byteBuf.readBytes(valueLength);

        K key = null;
        if (keyLength !=0) {
            byte[] keyBytes = new byte[keyByteBuf.readableBytes()];
            keyByteBuf.readBytes(keyBytes);
            try (ByteArrayInputStream bais = new ByteArrayInputStream(keyBytes)) {
                BinaryDecoder decoder = factory.binaryDecoder(bais, null);
                key = keyDatumReader.read(null, decoder);
            }
        }


        V value;
        byte[] valueBytes = new byte[valueByteBuf.readableBytes()];
        valueByteBuf.readBytes(valueBytes);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(valueBytes)) {
            BinaryDecoder decoder = factory.binaryDecoder(bais, null);

            value = valueDatumReader.read(null, decoder);
        }

        return new Pair<>(key, value);
    }


}
