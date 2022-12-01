/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.playground.datagen.model;

import com.github.javafaker.Faker;

import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * A supplier that generates an arbitrary transaction.
 */
public class StructureDataSupplier implements Supplier<StructuredData> {

    private final Random generator = new Random();
    private final Faker faker = new Faker(Locale.CHINA);

    private final Iterator<String> wxGroupId =
            Stream.generate(() -> Stream.of(1L, 2L, 3L, 4L, 5L).map(String::valueOf))
                    .flatMap(UnaryOperator.identity())
                    .iterator();

    private final Iterator<String> senderId =
            Stream.generate(() -> generator.nextInt(10))
                    .map(id -> "sender_" + id)
                    .iterator();

    private final Iterator<LocalDateTime> timestamps =
            Stream.iterate(
                            LocalDateTime.of(2022, 1, 1, 1, 0),
                            time -> time.plusMinutes(5).plusSeconds(generator.nextInt(58) + 1))
                    .iterator();

    @Override
    public StructuredData get() {
        StructuredData structuredData = new StructuredData();
        structuredData.uuid = UUID.randomUUID().toString();
        structuredData.wxGroupId = wxGroupId.next();
        structuredData.senderId = senderId.next();
        structuredData.sentence = buildFakeSentence();
        structuredData.timestamp = timestamps.next();
        structuredData.pictureUrl = faker.avatar().image();

        return structuredData;
    }

    private String buildFakeSentence() {
        StringBuilder sb = new StringBuilder();
        sb.append(faker.name().name());
        sb.append("擅长");
        sb.append(faker.job().field());
        sb.append("，工作在位于");
        sb.append(faker.address().fullAddress());
        sb.append("的");
        sb.append(faker.company().name());
        sb.append("，职位：");
        sb.append(faker.job().position());
        sb.append("。");
        return sb.toString();

    }
}
