package org.apache.sn.task.engine;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.util.Collector;
import org.apache.sn.task.engine.window.AllWindowAssigner;
import org.apache.sn.task.engine.window.SlidingWindowAssigner;
import org.apache.sn.task.engine.window.TumblingWindowAssigner;
import org.apache.sn.task.engine.window.WindowAssigner;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Functionality
 * 1. rule:dynamic update rule's state
 * 2. rule:restore rule state when restart flink
 * 3. msg: message would be filtered and grouped
 * 4. msg: each message goes through all rules
 * 5. agg: window finish -> compare result -> sink
 * <p>
 * Data Structure:
 * metric -->  rule1 --> group1 --> windowAssigner(origin value list) --> window1(max/min/sum)(trigger1)
 *             |       |                                              `---> window2 (avg)(trigger2)
 *             |       `--> group2 --> windowAssigner(origin value list) --> window3(trigger3)
 *             |                                                         `---> window4(trigger4)
 *              `rule2 --> group3 --> windowAssigner(origin value list) --> window5(trigger5)
 *             |
 *              `rule3 --> ....
 * all the window from one window assigner share the same origin value list(if needed)
 */
public class CEPEngine implements FlatMapFunction<Metric, BigDecimal> {

    //window assigner(a stateful object) for each group in this operator
    private final Map<String, WindowAssigner<Metric>> windowAssignerMap = Maps.newHashMap();
    @Override
    public void flatMap(Metric metric, Collector<BigDecimal> out) throws Exception {
        Rule rule = metric.getRule();
        getWindowAssigner(rule, metric.getGroupId(), out)
                .assignWindow(metric);
    }

    /**
     * get window assigner for the group
     *
     * @param rule    rule
     * @param groupId groupId
     * @param out     output pipeline
     * @return window assigner
     */
    private WindowAssigner<Metric> getWindowAssigner(Rule rule, String groupId, Collector<BigDecimal> out) {
        WindowAssigner<Metric> windowAssigner;
        if (MapUtils.isNotEmpty(windowAssignerMap) && windowAssignerMap.containsKey(groupId)) {
            windowAssigner = windowAssignerMap.get(groupId);
        } else {
            windowAssigner = createWindowAssigner(rule, out);
            windowAssignerMap.putIfAbsent(groupId, windowAssigner);
        }
        return windowAssigner;
    }

    /**
     * create window assigner by the rule config
     *
     * @param rule rule
     * @param out  output pipeline
     * @return new window assigner
     */
    private WindowAssigner<Metric> createWindowAssigner(Rule rule, Collector<BigDecimal> out) {
        WindowAssigner<Metric> windowAssigner;
        if (StringUtils.equals(rule.getWindowType(), "tumbling")) {
            windowAssigner = new TumblingWindowAssigner<>(rule, out);
        } else if (StringUtils.equals(rule.getWindowType(), "sliding")) {
            windowAssigner = new SlidingWindowAssigner<>(rule, out);
        } else {
            windowAssigner = new AllWindowAssigner<>(rule, out);
        }
        return windowAssigner;
    }
}
