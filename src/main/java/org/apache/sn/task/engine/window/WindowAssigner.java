package org.apache.sn.task.engine.window;


import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;

import java.math.BigDecimal;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

public abstract class WindowAssigner<IN extends Metric> {
    Rule rule;

    List<Window> windowList = Lists.newArrayList();

    final TreeMap<Long, BigDecimal> originValues = new TreeMap<>();

    public WindowAssigner(Rule rule) {
        this.rule = rule;
    }

    public List<Window> getWindowList() {
        return windowList;
    }

    public abstract List<Window> createWindowIfNecessary(IN in);

    /**
     * assign window for input value and return the window list that fit the input
     *
     * @param in input value
     * @return the window list that fit the input
     */
    public List<Window> assignWindow(IN in) {
        //get all window
        List<Window> newWindowList = createWindowIfNecessary(in);
        windowList.addAll(newWindowList);
        //if hit window, calculate it
        return windowList.stream()
                .filter(window -> window.isHit(in.getEventTime()))
                .peek(window -> {
                    //NeedAllElement aggr(e.g. AVG) store the value in originValues and calculate later
                    if (rule.getAggregatorFunctionType().isNeedAllElements()) {
                        originValues.put(in.getEventTime(), in.getMetric(rule.getAggregateFieldName()));
                    } else {
                        //DO NOT NeedAllElement aggr(e.g. sum/max/min) calculate right now
                        window.receive(in.getMetric(rule.getAggregateFieldName()));
                    }
                }).collect(Collectors.toList());
    }
}
