package org.apache.sn.task.engine;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.sn.task.engine.window.AllWindowAssigner;
import org.apache.sn.task.engine.window.SlidingWindowAssigner;
import org.apache.sn.task.engine.window.TumblingWindowAssigner;
import org.apache.sn.task.engine.window.WindowAssigner;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
 * |     |                                              `---> window2 (avg)(trigger2)
 * |      `--> group2 --> windowAssigner(origin value list) --> window3(trigger3)
 * |                                                    `---> window4(trigger4)
 * `rule2 --> group3 --> windowAssigner(origin value list) --> window5(trigger5)
 * |
 * `rule3 --> ....
 * all the window from one window assigner share the same origin value list(if needed)
 */
public class CEPEngine extends KeyedBroadcastProcessFunction<String, Metric, Rule, BigDecimal> {
    // broadcast state descriptor
    MapStateDescriptor<Integer, Rule> patternDesc;

    @Override
    public void open(Configuration conf) {
        patternDesc = new MapStateDescriptor<>("rules-desc", Types.INT, Types.POJO(Rule.class));
    }

    /**
     * deal with the metric value
     *
     * @param value metric value
     * @param ctx   broadcast context
     * @param out   output pipeline
     * @throws Exception exception
     */
    @Override
    public void processElement(Metric value, KeyedBroadcastProcessFunction<String, Metric, Rule, BigDecimal>.ReadOnlyContext ctx, Collector<BigDecimal> out) throws Exception {
        //get all the rules
        ReadOnlyBroadcastState<Integer, Rule> broadcastState = ctx.getBroadcastState(patternDesc);
        //iterate and hit the rules
        broadcastState.immutableEntries().forEach(ruleEntry -> {
            Preconditions.checkNotNull(ruleEntry, "rule entry must not be null");
            //if hit,calculate and update the value of state
            Rule rule = ruleEntry.getValue();
            if (Objects.nonNull(rule) && rule.isHit(value)) {
                //get groupId (ruleId+groupK1+groupK2)
                String groupId = getGroupId(value, rule);
                //get window assigner and assign window
                getWindowAssigner(rule, groupId, out).assignWindow(value);
            }
        });

    }

    /**
     * deal with the rule data
     *
     * @param rule rule value
     * @param ctx  broadcast context
     * @param out  output
     * @throws Exception exception
     */
    @Override
    public void processBroadcastElement(Rule rule, KeyedBroadcastProcessFunction<String, Metric, Rule, BigDecimal>.Context ctx, Collector<BigDecimal> out) throws Exception {
        BroadcastState<Integer, Rule> bcState = ctx.getBroadcastState(patternDesc);
        //remove the rule's data and resource when state is delete
        if (Objects.equals(Rule.RuleState.DELETE, rule.getRuleState())) {
            bcState.get(rule.getRuleId()).getWindowAssignerMap().clear();
            bcState.remove(rule.getRuleId());
        } else {
            //active and pause is still in the state
            bcState.put(rule.getRuleId(), rule);
        }
    }

    /**
     * groupId = ruleId + groupKey1 + groupKey2 +...
     *
     * @param value the metric value
     * @param rule  rule
     * @return groupId
     */
    private String getGroupId(Metric value, Rule rule) {
        return rule.getRuleId() + "_" + StringUtils.join(rule.getGroupingKeyNames().stream().map(value::getTag).collect(Collectors.toList()), "_");
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
        Map<String, WindowAssigner<Metric>> windowAssignerMap = rule.getWindowAssignerMap();
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
