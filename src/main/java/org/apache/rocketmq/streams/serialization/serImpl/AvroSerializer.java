package org.apache.rocketmq.streams.serialization.serImpl;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.rocketmq.streams.serialization.Serializer;

import java.io.ByteArrayOutputStream;

public class AvroSerializer<T> implements Serializer<T> {
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private final SpecificDatumWriter<T> writer = new SpecificDatumWriter<>();

    @Override
    public byte[] serialize(T data) throws Throwable {

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = this.encoderFactory.directBinaryEncoder(out, null);

            writer.write(data, encoder);
            encoder.flush();

            return out.toByteArray();
        }
    }
}
