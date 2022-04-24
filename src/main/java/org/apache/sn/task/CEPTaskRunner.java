package org.apache.sn.task;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.sn.task.engine.CEPEngine;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;

import java.math.BigDecimal;
import java.util.Objects;

public class CEPTaskRunner {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(4, TimeUnit.MINUTES), Time.of(10,TimeUnit.SECONDS)));
//        env.enableCheckpointing(5000);
        DataStreamSource<String> metricSource = env.socketTextStream("127.0.0.1", 9999);
        SingleOutputStreamOperator<Metric> metricStream = metricSource
                .map(CEPTaskRunner::parseMetric).name("parseMetric").filter(Objects::nonNull);

        MapStateDescriptor<Integer, Rule> ruleStateDescriptor = new MapStateDescriptor<>("rules", Types.INT, Types.POJO(Rule.class));
        DataStreamSource<String> ruleStream = env.socketTextStream("127.0.0.1", 8888);
        BroadcastStream<Rule> ruleBroadcastStream = ruleStream
                .map(CEPTaskRunner::parseRule).name("parseRule")
                .broadcast(ruleStateDescriptor);
        //join the metric stream and rule stream
        metricStream
                .keyBy(metric -> StringUtils.join(metric.getTags().values(), "_"))
                .connect(ruleBroadcastStream)
                .process(new CEPEngine()).name("cep_engine")
                .print()
                .disableChaining();
        env.execute("cep-task");
    }

    @SneakyThrows
    private static Rule parseRule(String line) {
        Rule rule;
        try {
            rule = JSONObject.parseObject(line, Rule.class);
        } catch (Exception e) {
            rule = null;
        }
        return rule;
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
