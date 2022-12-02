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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.expressions.TimeIntervalUnit;

import static org.apache.flink.table.api.Expressions.*;

public class DataReport {

  public static Table report(Table wxGroupEvent) {
    return wxGroupEvent
        .window(Tumble.over(lit(1).hour()).on("timestamp").as("send_time"))
        .groupBy($("wxGroupId"), $("senderId"), $("senf_time"))
        .select(
            $("wxGroupId").as("wx_group_id"),
            $("senderId").as("sender_id"),
            $("send_time"),
            $("senderId").count().as("send_count") // count 聚合，发送次数
        );
  }

  public static void main(String[] args) throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    TableEnvironment tEnv = TableEnvironment.create(settings);

    tEnv.executeSql("CREATE TABLE kafka_data_input (\n" +
        "    uuid       STRING,\n" +
        "    wxGroupId  STRING,\n" +
        "    senderId   STRING,\n" +
        "    sentence   STRING,\n" +
        "    pictureUrl STRING,\n" +
        "    `timestamp`  TIMESTAMP(3)\n" +
//            水位处理消息延迟
//                "    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND\n" +
        ") WITH (\n" +
        "    'connector' = 'kafka',\n" +
        "    'topic'     = 'transactions',\n" +
        "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
        "    'scan.startup.mode' = 'earliest-offset',\n" +

        "    'value.format'    = 'json',\n" +
        "    'value.json.fail-on-missing-field' = 'true',\n" +
        "    'value.fields-include' = 'ALL'\n" +
        ")");

    tEnv.executeSql("CREATE TABLE wx_group_active_report (\n" +
        "    wx_group_id    STRING,\n" +
        "    sender_id      STRING,\n" +
        "    send_time      TIMESTAMP(3),\n" +
        "    send_count     BIGINT\n," +
        "    PRIMARY KEY (wx_group_id, sender_id) NOT ENFORCED" +
        ") WITH (\n" +
        "  'connector'  = 'jdbc',\n" +
        "  'url'        = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
        "  'table-name' = 'wx_group_report',\n" +
        "  'driver'     = 'com.mysql.jdbc.Driver',\n" +
        "  'username'   = 'sql-demo',\n" +
        "  'password'   = 'demo-sql'\n" +
        ")");

    // from 输入表, 表名
    Table wxGroupEvent = tEnv.from("kafka_data_input");
    // report 处理输入的数据，然后执行插入到输出表
    report(wxGroupEvent).executeInsert("wx_group_active_report");
  }
}