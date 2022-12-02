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

package org.apache.flink.playgrounds.structuredata;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.table.api.Expressions.$;

/**
 * A unit test of the spend report.
 * If this test passes then the business
 * logic is correct.
 */
public class DataReportTest {

    private static final ZonedDateTime DATE_TIME = LocalDateTime.of(2020, 1, 1, 0, 0).atZone(ZoneId.systemDefault());

    @Test
    public void testReport() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        Table wxGroupEvent =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("uuid", DataTypes.STRING()),
                                DataTypes.FIELD("wxGroupId", DataTypes.STRING()),
                                DataTypes.FIELD("senderId", DataTypes.STRING()),
//                                DataTypes.FIELD("sentence", DataTypes.STRING()),
                                DataTypes.FIELD("timestamp", DataTypes.BIGINT())),
                        Row.of("1", "1", "100", DATE_TIME.plusMinutes(12).toInstant().toEpochMilli()),
                        Row.of("2", "1", "200", DATE_TIME.plusMinutes(47).toInstant().toEpochMilli()),
                        Row.of("3", "1", "100", DATE_TIME.plusMinutes(36).toInstant().toEpochMilli()),
                        Row.of("4", "1", "200", DATE_TIME.plusMinutes(3).toInstant().toEpochMilli()),
                        Row.of("5", "1", "300", DATE_TIME.plusMinutes(8).toInstant().toEpochMilli()),
                        Row.of("1", "2", "100", DATE_TIME.plusMinutes(53).toInstant().toEpochMilli()),
                        Row.of("2", "2", "200", DATE_TIME.plusMinutes(32).toInstant().toEpochMilli()),
                        Row.of("3", "2", "100", DATE_TIME.plusMinutes(31).toInstant().toEpochMilli()),
                        Row.of("4", "2", "300", DATE_TIME.plusMinutes(19).toInstant().toEpochMilli()),
                        Row.of("5", "2", "300", DATE_TIME.plusMinutes(42).toInstant().toEpochMilli()));
//        wxGroupEvent.execute().print();
//        wxGroupEvent
//                .groupBy($("wxGroupId"), $("senderId"))
//                .select(
//                        $("wxGroupId").as("wx_group_id"),
//                        $("senderId").as("sender_id"),
//                        $("senderId").count().as("send_count") // count 聚合，发送次数
//                )
//                .execute().print();
        try {
            TableResult results = DataReport.report(wxGroupEvent).execute();

            MatcherAssert.assertThat(
                    materialize(results),
                    Matchers.containsInAnyOrder(
                            Row.of("1", "100", 2L),
                            Row.of("1", "200", 2L),
                            Row.of("1", "300", 1L),
                            Row.of("2", "100", 2L),
                            Row.of("2", "200", 1L),
                            Row.of("2", "300", 2L)));
        } catch (UnimplementedException e) {
            Assume.assumeNoException("The walkthrough has not been implemented", e);
        }
    }

    private static List<Row> materialize(TableResult results) {
        try (CloseableIterator<Row> resultIterator = results.collect()) {
            return StreamSupport
                    .stream(Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.ORDERED), false)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to materialize results", e);
        }
    }
}
