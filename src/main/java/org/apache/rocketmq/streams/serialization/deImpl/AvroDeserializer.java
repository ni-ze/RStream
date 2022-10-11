package org.apache.rocketmq.streams.serialization.deImpl;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.rocketmq.streams.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

public class AvroDeserializer<T> implements Deserializer<T> {
    private DecoderFactory factory;
    private DatumReader<T> datumReader;

    public AvroDeserializer() {
        factory = DecoderFactory.get();
        datumReader = new SpecificDatumReader<>();
    }

    @Override
    public T deserialize(byte[] data) throws Throwable {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        BinaryDecoder decoder = factory.binaryDecoder(bais, null);
        ByteBuffer buffer = ByteBuffer.allocate(16);

        try {
            decoder.readBytes(buffer);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datumReader.read(null, decoder);
    }
}
