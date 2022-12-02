package org.apache.flink.playground.datagen.model;

import java.time.LocalDateTime;

/** A simple financial transaction. */
public class StructuredData {
  public String uuid;

  public String wxGroupId;

  public String senderId;

  public String sentence;

  public String pictureUrl;

  public LocalDateTime timestamp;

  @Override
  public String toString() {
    return "StructuredData{"
        + "uuid='"
        + uuid
        + '\''
        + ", wxGroupId='"
        + wxGroupId
        + '\''
        + ", senderId='"
        + senderId
        + '\''
        + ", sentence='"
        + sentence
        + '\''
        + ", pictureUrl='"
        + pictureUrl
        + '\''
        + ", timestamp="
        + timestamp
        + '}';
  }
}
