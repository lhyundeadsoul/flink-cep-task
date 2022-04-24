package org.apache.sn.task.engine.window;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;

import java.math.BigDecimal;
import java.util.List;

public class TumblingWindowAssigner<IN extends Metric> extends WindowAssigner<IN> {
    private long earliestTimestamp = Long.MIN_VALUE;

    public TumblingWindowAssigner(Rule rule, Collector<BigDecimal> out) {
        super(rule, out);
    }

    @Override
    public List<Window> createWindowIfNecessary(IN in) {
        long begin;
        long end;
        //if never open a window
        if (earliestTimestamp == Long.MIN_VALUE) {
            earliestTimestamp = in.getEventTime();
            begin = in.getEventTime();
            end = in.getEventTime() + rule.getWindowMillis();
        } else {
            //there is already some windows
            begin = in.getEventTime() - (in.getEventTime() - earliestTimestamp) % rule.getWindowMillis();
            if (in.getEventTime() > earliestTimestamp) {
                end = begin + rule.getWindowMillis();
            } else {
                end = begin - rule.getWindowMillis();
            }
        }
        Window window = new Window(begin, end, rule.getAggregatorFunctionType(), this);
        List<Window> newWindowList = Lists.newArrayList();
        if (!windowList.contains(window)) {
            newWindowList.add(window);
        }
        return newWindowList;
    }
}
