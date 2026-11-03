package com.shop.order.outbox;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/** Converts Avro events to and from Avro's JSON encoding (used for the outbox payload). */
public final class AvroJson {

    private AvroJson() {
    }

    public static String toJson(SpecificRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
            SpecificDatumWriter<SpecificRecord> writer =
                    new SpecificDatumWriter<>(record.getSchema(), SpecificData.getForClass(record.getClass()));
            writer.write(record, encoder);
            encoder.flush();
            return out.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T extends SpecificRecord> T fromJson(String json, Class<T> type) {
        try {
            SpecificData data = SpecificData.getForClass(type);
            Schema schema = data.getSchema(type);
            SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema, schema, data);
            return reader.read(null, DecoderFactory.get().jsonDecoder(schema, json));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}