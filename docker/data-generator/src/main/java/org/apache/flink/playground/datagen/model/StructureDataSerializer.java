package org.apache.flink.playground.datagen.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public class StructureDataSerializer implements Serializer<StructuredData> {
  private final ObjectMapper om = new ObjectMapper();

  @Override
  public void configure(Map<String, ?> map, boolean b) {
    JavaTimeModule javaTimeModule = new JavaTimeModule();
    javaTimeModule.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer());
    om.registerModule(javaTimeModule);
  }

  @Override
  public byte[] serialize(String s, StructuredData structuredData) {
    try {
      return om.writeValueAsBytes(structuredData);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public class LocalDateTimeSerializer extends JsonSerializer<LocalDateTime> {
    @Override
    public void serialize(
        LocalDateTime localDateTime,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
        throws IOException {
      jsonGenerator.writeNumber(
          localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }
  }

  @Override
  public void close() {}
}
