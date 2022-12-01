package org.apache.flink.playground.datagen;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import org.apache.flink.playground.datagen.model.StructuredData;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

public class StructureDataDeserializer implements Deserializer<StructuredData> {
    private final ObjectMapper om = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer());
        om.registerModule(javaTimeModule);
    }

    @Override
    public StructuredData deserialize(String s, byte[] bytes) {
        try {
            return om.readValue(bytes, StructuredData.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public class LocalDateTimeDeserializer extends JsonDeserializer<LocalDateTime> {

        @Override
        public LocalDateTime deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException {
            long timestamp = jsonParser.getLongValue();
            if (timestamp > 0) {
                return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
            }
            return null;
        }
    }

    @Override
    public void close() {

    }
}
