package org.apache.sn.task.engine.window;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class WindowTest {

    @Test
    public void testResult() {
        Rule rule = new Rule();
        rule.setAggregateFieldName("cpu");
        rule.setLimitOperatorType(Rule.LimitOperatorType.GREATER);
        rule.setLimit(BigDecimal.valueOf(5));
        WindowAssigner<? extends Metric> windowAssigner = new TumblingWindowAssigner<>(rule, null);
        Window window = new Window(1, 10, Rule.AggregatorFunctionType.SUM, windowAssigner);
        Metric metric1 = new Metric();
        metric1.setEventTime(1);
        Map<String, BigDecimal> metricMap = Maps.newHashMap();
        metricMap.put("cpu",BigDecimal.valueOf(5));
        metric1.setMetrics(metricMap);
        window.receive(metric1);
        Metric metric2 = new Metric();
        metric2.setEventTime(5);
        Map<String, BigDecimal> metricMap2 = Maps.newHashMap();
        metricMap2.put("cpu",BigDecimal.valueOf(7));
        metric2.setMetrics(metricMap2);
        window.receive(metric2);
        assertThat(window.result()).isEqualTo(BigDecimal.valueOf(12));

        Window window2 = new Window(1, 10, Rule.AggregatorFunctionType.AVG, windowAssigner);
        window2.receive(metric1);
        window2.receive(metric2);
        assertThat(window2.result()).isEqualTo(BigDecimal.valueOf(6));
    }

    @Test
    public void testIsHit() {
        WindowAssigner<? extends Metric> windowAssigner = new TumblingWindowAssigner<>(null, null);
        Window window = new Window(1, 10, Rule.AggregatorFunctionType.SUM, windowAssigner);
        Metric metric1 = new Metric();
        metric1.setEventTime(1);
        assertThat(window.isHit(metric1.getEventTime())).isEqualTo(true);
        metric1.setEventTime(11);
        assertThat(window.isHit(metric1.getEventTime())).isEqualTo(false);
    }

}