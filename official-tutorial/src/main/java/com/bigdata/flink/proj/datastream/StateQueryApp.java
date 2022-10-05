package com.bigdata.flink.proj.datastream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.util.concurrent.CompletableFuture;

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/7 11:54 上午
 * @desc:
 */
public class StateQueryApp {

    public static void main(String[] args) throws Exception {
        JobID jobId = JobID.fromHexString(args[0]); // webUI 上线上的 ID
        QueryableStateClient client = new QueryableStateClient("localhost", 9069);

        // the state descriptor of the state to be fetched.
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "num-sum",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));

        // nc -l 9001 输入个数为奇数才有状态，偶数要输出结果，清空状态
        CompletableFuture<ValueState<Tuple2<Long, Long>>> resultFuture =
                client.getKvState(jobId, "myQueryableState", 1L, BasicTypeInfo.LONG_TYPE_INFO, descriptor);

        Tuple2<Long, Long> value = resultFuture.get().value();
        System.out.println(value.f0+","+value.f1);
    }
}
