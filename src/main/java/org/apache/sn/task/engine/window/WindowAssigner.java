package org.apache.sn.task.engine.window;


import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;
import org.apache.sn.task.engine.trigger.Trigger;
import org.apache.sn.task.engine.trigger.TriggerCenter;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;

import java.math.BigDecimal;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

public abstract class WindowAssigner<IN extends Metric> {
    /**
     * trigger register center
     */
    TriggerCenter triggerCenter = new TriggerCenter();
    /**
     * get window config by this rule
     */
    Rule rule;

    /**
     * windows created by this assigner
     */
    List<Window> windowList = Lists.newArrayList();

    /**
     * collector of window result
     */
    Collector<BigDecimal> out;

    /**
     * origin value storage for each window
     */
    final TreeMap<Long, BigDecimal> originValues = new TreeMap<>();

    public WindowAssigner(Rule rule, Collector<BigDecimal> out) {
        this.rule = rule;
        this.out = out;
    }
    /**
     * create window for the input value if necessary
     * @param in  input value
     * @return window list
     */
    public abstract List<Window> createWindowIfNecessary(IN in);

    /**
     * assign window for input value and return the window list that fit the input
     *
     * @param in input value
     * @return the window list that fit the input
     */
    public List<Window> assignWindow(IN in) {
        //create new window if necessary
        List<Window> newWindowList = createWindowIfNecessary(in);
        //set trigger for each new window
        if (CollectionUtils.isNotEmpty(newWindowList)){
            newWindowList.forEach(window -> {
                Trigger trigger = new Trigger(window);
                triggerCenter.register(trigger);
            });
        }
        windowList.addAll(newWindowList);
        //if hit window, receive it
        return windowList.stream()
                .filter(window -> window.isHit(in.getEventTime()))
                .peek(window -> window.receive(in))
                .collect(Collectors.toList());
    }

    public Rule getRule() {
        return rule;
    }

    public List<Window> getWindowList() {
        return windowList;
    }

    TreeMap<Long, BigDecimal> getOriginValues() {
        return originValues;
    }

    public Collector<BigDecimal> getOut() {
        return out;
    }

}
