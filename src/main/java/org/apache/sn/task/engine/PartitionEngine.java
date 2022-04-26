package org.apache.sn.task.engine;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Functionality
 * 1. rule:dynamic update rule's state
 * 2. rule:restore rule state when restart flink
 * 3. msg: message would be filtered and grouped
 * 4. msg: each message goes through all rules
 * 5. agg: window finish -> compare result -> sink
 */
public class PartitionEngine extends BroadcastProcessFunction<Metric, Rule, Metric> {
    // broadcast state descriptor
    MapStateDescriptor<Integer, Rule> patternDesc;

    @Override
    public void open(Configuration conf) {
        patternDesc = new MapStateDescriptor<>("rules-desc", Types.INT, Types.POJO(Rule.class));
    }

    @Override
    public void processElement(Metric metric, BroadcastProcessFunction<Metric, Rule, Metric>.ReadOnlyContext ctx, Collector<Metric> out) throws Exception {
        //get all the rules
        ReadOnlyBroadcastState<Integer, Rule> broadcastState = ctx.getBroadcastState(patternDesc);
        //iterate and hit the rules
        broadcastState.immutableEntries().forEach(ruleEntry -> {
            Preconditions.checkNotNull(ruleEntry, "rule entry must not be null");
            //if hit,calculate and update the metric of state
            Rule rule = ruleEntry.getValue();
            if (Objects.nonNull(rule) && rule.isHit(metric)) {
                //get groupId (ruleId+groupK1+groupK2)
                metric.setGroupId(getGroupId(metric, rule));
                metric.setRule(rule);
                out.collect(metric);
            }
        });
    }

    @Override
    public void processBroadcastElement(Rule rule, BroadcastProcessFunction<Metric, Rule, Metric>.Context ctx, Collector<Metric> out) throws Exception {
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
     * @param metric the metric value
     * @param rule  rule
     * @return groupId
     */
    private String getGroupId(Metric metric, Rule rule) {
        return rule.getRuleId()
                + "_"
                + StringUtils.join(
                        rule.getGroupingKeyNames()
                                .stream()
                                .map(metric::getTag)
                                .collect(Collectors.toList()),
                "_");
    }
}
