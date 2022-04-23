package org.apache.sn.task.engine;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Functionality
 * 1. rule:dynamic update rule's state
 * 2. rule:restore rule state when restart flink
 * 3. msg: message would be filtered and grouped
 * 4. msg: each message goes through all rules
 * 5. agg: window finish -> compare result -> sink
 * 6. eventTime?
 * <p>
 * UnitTest
 */
public class CEPEngine extends KeyedBroadcastProcessFunction<String, Metric, Rule, BigDecimal> {
    // handle for value state(Use List here, because you don't know the number of parameters that aggr_function needs, e.g. AVG ==> List[0]=sum, List[1]=num, avg=sum/num)
    MapState<String, List<BigDecimal>> valueMapState;
    // broadcast state descriptor
    MapStateDescriptor<Integer, Rule> patternDesc;

    @Override
    public void open(Configuration conf) {
        valueMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("values", Types.STRING, Types.LIST(Types.BIG_DEC)));
        patternDesc = new MapStateDescriptor<>("rules", Types.INT, Types.POJO(Rule.class));
    }

    @Override
    public void processElement(Metric value, KeyedBroadcastProcessFunction<String, Metric, Rule, BigDecimal>.ReadOnlyContext ctx, Collector<BigDecimal> out) throws Exception {
        //get all the rules
        ReadOnlyBroadcastState<Integer, Rule> broadcastState = ctx.getBroadcastState(patternDesc);
        //iterate and hit the rules,
        broadcastState.immutableEntries().forEach(ruleEntry -> {
            Preconditions.checkNotNull(ruleEntry, "rule entry must not be null");
            //if hit,calculate and update the value of state
            Rule rule = ruleEntry.getValue();
            if (isHit(value, rule)) {
                try {
                    //get groupId (ruleId+groupK1+groupK2)
                    String groupId = rule.getRuleId() + "_" + StringUtils.join(rule.getGroupingKeyNames().stream().map(value::getTag).collect(Collectors.toList()), "_");
                    //get old value and calculate new value
                    List<BigDecimal> newValue = calcUpdateValue(
                            valueMapState.get(groupId),
                            rule.getAggregatorFunctionType(),
                            value.getMetric(rule.getAggregateFieldName())
                    );
                    //update
                    valueMapState.put(groupId, newValue);
                    //then, compare and sink if necessary
                    BigDecimal finalValue = calcFinalValue(rule.getAggregatorFunctionType(), newValue);
                    if (rule.apply(finalValue)) {
                        out.collect(finalValue);
                        //todo window?
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }

    /**
     * calculate the final value which is used to compare
     *
     * @param aggregatorFunctionType aggrType
     * @param newValue               new value
     * @return final value
     */
    private BigDecimal calcFinalValue(Rule.AggregatorFunctionType aggregatorFunctionType, List<BigDecimal> newValue) {
        BigDecimal finalValue;
        if (Rule.AggregatorFunctionType.AVG.equals(aggregatorFunctionType)) {
            finalValue = newValue.get(0).divide(newValue.get(1), RoundingMode.CEILING);
        } else {
            finalValue = newValue.get(0);
        }
        return finalValue;
    }

    /**
     * calculate the value for update MapState
     *
     * @param currentValueList       current
     * @param aggregatorFunctionType aggrType
     * @param deltaValue             delta
     * @return new value for update
     */
    private List<BigDecimal> calcUpdateValue(List<BigDecimal> currentValueList, Rule.AggregatorFunctionType aggregatorFunctionType, BigDecimal deltaValue) {
        if (Objects.isNull(currentValueList)) {
            currentValueList = Lists.newArrayList();
            currentValueList.add(BigDecimal.ZERO);
            currentValueList.add(BigDecimal.ZERO);
        }
        switch (aggregatorFunctionType) {
            case AVG:
                currentValueList.set(0, currentValueList.get(0).add(deltaValue));
                currentValueList.set(1, currentValueList.get(1).add(BigDecimal.ONE));
                return currentValueList;
            case MAX:
                currentValueList.set(0, currentValueList.get(0).max(deltaValue));
                return currentValueList;
            case MIN:
                currentValueList.set(0, currentValueList.get(0).min(deltaValue));
                return currentValueList;
            case SUM:
                currentValueList.set(0, currentValueList.get(0).add(deltaValue));
                return currentValueList;
            default:
                throw new RuntimeException("Unknown aggregatorFunctionType: " + aggregatorFunctionType);
        }
    }

    private boolean isHit(Metric metric, Rule rule) {
        Preconditions.checkNotNull(metric, "metric must not be null");
        Preconditions.checkNotNull(rule, "rule " + rule + " must not be null");
        return Objects.equals(Rule.RuleState.ACTIVE, rule.getRuleState()) && metric.getTags().keySet().containsAll(rule.getGroupingKeyNames());
    }

    @Override
    public void processBroadcastElement(Rule rule, KeyedBroadcastProcessFunction<String, Metric, Rule, BigDecimal>.Context ctx, Collector<BigDecimal> out) throws Exception {
        BroadcastState<Integer, Rule> bcState = ctx.getBroadcastState(patternDesc);
        if (Objects.equals(Rule.RuleState.DELETE, rule.getRuleState())) {
            bcState.remove(rule.getRuleId());
        } else {//active and pause is still in the state
            bcState.put(rule.getRuleId(), rule);
        }
        //todo reset window?
    }
}
