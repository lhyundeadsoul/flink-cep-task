package org.apache.sn.task.engine.window;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;

import java.math.BigDecimal;
import java.util.List;

public class AllWindowAssigner<IN extends Metric> extends WindowAssigner<IN> {
    private boolean isNoWindow = Boolean.TRUE;
    public AllWindowAssigner(Rule rule, Collector<BigDecimal> out) {
        super(rule, out);
    }

    @Override
    public List<Window> createWindowIfNecessary(IN in) {
        List<Window> newWindowList = Lists.newArrayList();
        if (isNoWindow) {
            newWindowList.add(new Window(Long.MIN_VALUE, Long.MAX_VALUE, rule.getAggregatorFunctionType(), this));
            isNoWindow = Boolean.FALSE;
        }
        return newWindowList;
    }
}
