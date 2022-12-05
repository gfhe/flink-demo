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

import org.apache.flink.table.api.*;
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
import static org.apache.flink.table.api.Expressions.lit;

/**
 * A unit test of the spend report.
 * If this test passes then the business
 * logic is correct.
 */
public class DataReportTest {

  private static final LocalDateTime DATE_TIME = LocalDateTime.of(2020, 1, 1, 0, 0);

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
                DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP(3))),
            Row.of("1", "1", "100", DATE_TIME.plusMinutes(12)),
            Row.of("2", "1", "200", DATE_TIME.plusMinutes(47)),
            Row.of("3", "1", "100", DATE_TIME.plusMinutes(36)),
            Row.of("4", "1", "200", DATE_TIME.plusMinutes(3)),
            Row.of("5", "1", "300", DATE_TIME.plusMinutes(8)),
            Row.of("1", "2", "100", DATE_TIME.plusMinutes(53)),
            Row.of("2", "2", "200", DATE_TIME.plusMinutes(32)),
            Row.of("3", "2", "100", DATE_TIME.plusMinutes(31)),
            Row.of("4", "2", "300", DATE_TIME.plusMinutes(19)),
            Row.of("5", "2", "300", DATE_TIME.plusMinutes(42)));
    wxGroupEvent.printSchema();
    // 窗口函数
    wxGroupEvent
        .window(Tumble.over(lit(1).minute()).on($("timestamp")).as("send_time"))
        .groupBy($("wxGroupId"),$("senderId"), $("send_time"))
        .select(
            $("wxGroupId").as("wx_group_id"),
            $("senderId").as("sender_id"),
            $("send_time").start().as("send_time")
        )
        .execute().print();
//    wxGroupEvent
//        .window(Tumble.over(lit(1).hour()).on($("timestamp")).as("send_time"))
//        .groupBy($("wxGroupId"), $("senderId"), $("send_time"))
//        .select(
//            $("wxGroupId").as("wx_group_id"),
//            $("senderId").as("sender_id"),
//            $("senderId").count().as("send_count") // count 聚合，发送次数
//        )
//        .execute().print();

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
