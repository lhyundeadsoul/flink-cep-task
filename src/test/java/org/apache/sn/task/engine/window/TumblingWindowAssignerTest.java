package org.apache.sn.task.engine.window;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TumblingWindowAssignerTest {

    @Test
    public void testCreateWindowIfNecessary() {
        // create new window
        Rule rule = new Rule();
        rule.setAggregateFieldName("cpu");
        rule.setAggregatorFunctionType(Rule.AggregatorFunctionType.SUM);
        rule.setWindowMinutes(4);
        TumblingWindowAssigner<Metric> tumblingWindowAssigner = new TumblingWindowAssigner<>(rule, null);
        Metric in = new Metric();
        in.setEventTime(1);
        Map<String, BigDecimal> map = Maps.newHashMap();
        map.put("cpu", BigDecimal.valueOf(5));
        in.setMetrics(map);
        List<Window> windows = tumblingWindowAssigner.assignWindow(in);

        assertThat(windows)
                .isNotEmpty()
                .hasSize(1)
                .extractingResultOf("getBeginTimestamp").first().isEqualTo(1L);
        assertThat(windows)
                .isNotEmpty()
                .hasSize(1)
                .extractingResultOf("getEndTimestamp").first().isEqualTo((long) (4 * 60 * 1000 + 1));

        //do not need to create new window
        Metric in2 = new Metric();
        in2.setEventTime(2);
        List<Window> windows2 = tumblingWindowAssigner.assignWindow(in2);
        assertThat(windows2).isEqualTo(windows);

        //create next window
        Metric in3 = new Metric();
        in3.setEventTime(4 * 60 * 1000 + 1+100);
        List<Window> windows3 = tumblingWindowAssigner.assignWindow(in3);
        assertThat(windows3)
                .doesNotContainAnyElementsOf(windows)
                .hasSize(1)
                .extractingResultOf("getBeginTimestamp").first().isEqualTo((long) (4 * 60 * 1000 + 1));
        assertThat(windows3)
                .doesNotContainAnyElementsOf(windows)
                .hasSize(1)
                .extractingResultOf("getEndTimestamp").first().isEqualTo((long) (4 * 60 * 1000 + 1 + 4 * 60 * 1000));

    }
}