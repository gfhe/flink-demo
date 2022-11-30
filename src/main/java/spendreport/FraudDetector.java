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

package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.io.IOException;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {


    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

    //	flink保存keyed value  的可靠原语
    private transient ValueState<Boolean> flagState;
    //    定时器
    private transient ValueState<Long> timerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 从 context 中 获取value state。
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<Boolean>("flag", Types.BOOLEAN);
        this.flagState = getRuntimeContext().getState(flagDescriptor);
        ValueStateDescriptor<Long> timeDescriptor = new ValueStateDescriptor<Long>("timeer", Types.LONG);
        this.timerState = getRuntimeContext().getState(timeDescriptor);

    }

    /**
     * 业务
     * 1. 小定时器和标记设置；
     * 2. 时间内不管触发没触发大额，清理掉计时器和状态；fla清理alert， ，时，存在flag 大额了发现* 3.
     * <p>
     * 当标记状态被设置为 true 时，设置一个在当前时间一分钟后触发的定时器。
     * 当定时器被触发时，重置标记状态。
     * 当标记状态被重置时，删除定时器。
     *
     * @param transaction
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement(Transaction transaction, Context context, Collector<Alert> collector) throws Exception {
        Boolean lastTransactionWasSmall = this.flagState.value();

        if (lastTransactionWasSmall != null && lastTransactionWasSmall == Boolean.TRUE) {
            if (transaction.getAmount() > LARGE_AMOUNT) {
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());

                collector.collect(alert);
            }
            // 在时间过期前发现异常，主动触发清理
            cleanup(context);
        }

        if (transaction.getAmount() < SMALL_AMOUNT) {
            this.flagState.update(true);

            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            // 注册定时器
            context.timerService().registerProcessingTimeTimer(timer);
            this.timerState.update(timer);
        }
    }

    private void cleanup(KeyedProcessFunction<Long, Transaction, Alert>.Context ctx) throws IOException {
        // delete timer
        Long timer = timerState.value();
        ctx.timerService().deleteProcessingTimeTimer(timer);

        // clean up all state
        timerState.clear();
        flagState.clear();
    }

    // 响应定时触发事件
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, Transaction, Alert>.OnTimerContext ctx, Collector<Alert> out) throws Exception {
        this.timerState.clear();
        this.flagState.clear();
    }
}
