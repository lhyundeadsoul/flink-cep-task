package org.apache.sn.task.engine.window;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.*;

public class AllWindowAssignerTest {

    @Test
    public void testCreateWindowIfNecessary() {
        // create new window
        Rule rule = new Rule();
        rule.setAggregateFieldName("cpu");
        rule.setAggregatorFunctionType(Rule.AggregatorFunctionType.SUM);
        rule.setWindowMinutes(4);
        AllWindowAssigner<Metric> allWindowAssigner = new AllWindowAssigner<>(rule, null);
        Metric in = new Metric();
        in.setEventTime(1);
        Map<String, BigDecimal> map = Maps.newHashMap();
        map.put("cpu", BigDecimal.valueOf(5));
        in.setMetrics(map);
        List<Window> windows = allWindowAssigner.assignWindow(in);

        assertThat(windows)
                .isNotEmpty()
                .hasSize(1)
                .extractingResultOf("getBeginTimestamp").first().isEqualTo(Long.MIN_VALUE);
        assertThat(windows)
                .isNotEmpty()
                .hasSize(1)
                .extractingResultOf("getEndTimestamp").first().isEqualTo(Long.MAX_VALUE);

        //do not need to create new window
        Metric in2 = new Metric();
        in2.setEventTime(2);
        List<Window> windows2 = allWindowAssigner.assignWindow(in2);
        assertThat(windows2).isEqualTo(windows);

    }
}