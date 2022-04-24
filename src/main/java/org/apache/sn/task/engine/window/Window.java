package org.apache.sn.task.engine.window;

import lombok.Data;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.util.Collector;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Just a normal window which only has :
 * 1. begin/end timestamp
 * 2. aggr type
 * 3. aggr trigger
 */
@Data
public class Window {
    public Window(long beginTimestamp, long endTimestamp, Rule.AggregatorFunctionType aggregatorFunctionType, WindowAssigner<? extends Metric> windowAssigner) {
        this.beginTimestamp = beginTimestamp;
        this.endTimestamp = endTimestamp;
        this.aggregatorFunctionType = aggregatorFunctionType;
        this.windowAssigner = windowAssigner;
    }

    private WindowAssigner<? extends Metric> windowAssigner;
    private long beginTimestamp;
    private long endTimestamp;
    private Rule.AggregatorFunctionType aggregatorFunctionType;
    private BigDecimal result;

    /**
     * receive value for this window
     * NeedAllElement aggr(e.g. AVG) store the value in originValues and calculate later
     * DO NOT NeedAllElement aggr(e.g. sum/max/min) calculate right now
     * @param input     value
     */
    public void receive(Metric input) {
        String aggregateFieldName = getWindowAssigner().getRule().getAggregateFieldName();
        if (aggregatorFunctionType.noNeedAllElements()) {
            result = aggregate(result, input.getMetric(aggregateFieldName));
        } else {
            windowAssigner.getOriginValues().put(input.getEventTime(), input.getMetric(aggregateFieldName));
        }
    }

    /**
     * get, compare and output the result
     * @return result
     */
    public BigDecimal result() {
        //get result
        if (aggregatorFunctionType.isNeedAllElements()) {
            result = aggrWithOriginValues(windowAssigner.getOriginValues(), aggregatorFunctionType);
        }
        Collector<BigDecimal> out = getWindowAssigner().getOut();
        // compare and output
        if (getWindowAssigner().getRule().apply(result) && Objects.nonNull(out)) {
            out.collect(result);
        }
        return result;
    }

    public boolean isHit(long timestamp) {
        return timestamp >= beginTimestamp && timestamp < endTimestamp;
    }

    /**
     * calculate the value for update MapState with origin values
     *
     * @param originValues           origin values
     * @param aggregatorFunctionType aggr type
     * @return result calculated by origin values
     */
    private BigDecimal aggrWithOriginValues(TreeMap<Long, BigDecimal> originValues, Rule.AggregatorFunctionType aggregatorFunctionType) {
        if (Objects.equals(aggregatorFunctionType, Rule.AggregatorFunctionType.AVG) && MapUtils.isNotEmpty(originValues)) {
            return originValues
                    .subMap(beginTimestamp, endTimestamp)
                    .values()
                    .stream()
                    .reduce(BigDecimal::add)
                    .orElse(BigDecimal.ZERO)
                    .divide(BigDecimal.valueOf(originValues.size()), RoundingMode.CEILING)
                    ;
        }
        return BigDecimal.ZERO;
    }

    /**
     * calculate the value for update MapState without origin values
     *
     * @param currentValue current
     * @param deltaValue   delta
     * @return new value for update
     */
    public BigDecimal aggregate(BigDecimal currentValue, BigDecimal deltaValue) {
        if (Objects.isNull(currentValue)) {
            if (Objects.equals(aggregatorFunctionType, Rule.AggregatorFunctionType.MIN)){
                currentValue = BigDecimal.valueOf(Long.MAX_VALUE);
            } else {
                currentValue = BigDecimal.ZERO;
            }
        }
        if (Objects.isNull(deltaValue)) {
            deltaValue = BigDecimal.ZERO;
        }
        switch (aggregatorFunctionType) {
//            case AVG:  avg
//                currentValue.set(0, currentValue.get(0).add(deltaValue));
//                currentValue.set(1, currentValue.get(1).add(BigDecimal.ONE));
//                return currentValue;
            case MAX:
                currentValue = currentValue.max(deltaValue);
                return currentValue;
            case MIN:
                currentValue = currentValue.min(deltaValue);
                return currentValue;
            case SUM:
                currentValue = currentValue.add(deltaValue);
                return currentValue;
            default:
                throw new RuntimeException("Unknown aggregatorFunctionType: " + aggregatorFunctionType);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Window window = (Window) o;
        return beginTimestamp == window.beginTimestamp && endTimestamp == window.endTimestamp && aggregatorFunctionType == window.aggregatorFunctionType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(beginTimestamp, endTimestamp, aggregatorFunctionType);
    }
}
