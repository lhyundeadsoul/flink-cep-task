package org.apache.sn.task;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.sn.task.engine.CEPEngine;
import org.apache.sn.task.engine.PartitionEngine;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class CEPTaskRunner {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        env.setStateBackend(new FsStateBackend("file:///Users/lihongyuinfo/Sources/flink-cep-task/checkpoints"));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.of(4, TimeUnit.SECONDS), Time.of(10,TimeUnit.SECONDS)));
//        env.enableCheckpointing(120000);
//        env.setBufferTimeout(120000);
        DataStreamSource<String> metricSource = env.socketTextStream("127.0.0.1", 9999);
        SingleOutputStreamOperator<Metric> metricStream = metricSource
                .map(CEPTaskRunner::parseMetric).name("parseMetric")
                .filter(Objects::nonNull);

        MapStateDescriptor<Integer, Rule> ruleStateDescriptor = new MapStateDescriptor<>("rules-desc", Types.INT, Types.POJO(Rule.class));
        DataStreamSource<String> ruleStream = env.socketTextStream("127.0.0.1", 8888);
        BroadcastStream<Rule> ruleBroadcastStream = ruleStream
                .map(CEPTaskRunner::parseRule).name("parseRule")
                .filter(rule -> Objects.nonNull(rule))
                .broadcast(ruleStateDescriptor);
        //join the metric stream and rule stream
        metricStream
                .connect(ruleBroadcastStream)
                .process(new PartitionEngine()).name("partition_engine")
                .keyBy(Metric::getGroupId)
                .flatMap(new CEPEngine())
                .print()
                .disableChaining();
        env.execute("cep-task");
    }

    @SneakyThrows
    private static Rule parseRule(String line) {
        return JSONObject.parseObject(line, Rule.class);
    }

    @SneakyThrows
    private static Metric parseMetric(String line) {
        Metric metric;
        try {
            JSONObject jsonObject = JSONObject.parseObject(line);
            metric = new Metric();
            Metric finalMetric = metric;
            metric.setEventTime(jsonObject.getLong("eventTime"));
            jsonObject.entrySet().stream().filter(entry -> entry.getKey().contains("t_")).forEach(entry -> finalMetric.getTags().putIfAbsent(entry.getKey(), entry.getValue().toString()));
            jsonObject.entrySet().stream().filter(entry -> !entry.getKey().contains("t_") && !entry.getKey().equals("eventTime")).forEach(entry -> finalMetric.getMetrics().putIfAbsent(entry.getKey(), BigDecimal.valueOf(Integer.parseInt(entry.getValue().toString()))));
        } catch (Exception e) {
            metric = null;
        }
        return metric;
    }
}
