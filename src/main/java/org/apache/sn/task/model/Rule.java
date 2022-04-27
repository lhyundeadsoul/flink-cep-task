package org.apache.sn.task.model;

import lombok.Data;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

@Data
public class Rule {
    private Integer ruleId;
    private RuleState ruleState;
    private Integer windowMinutes;
    private Integer windowSlideMinute;
    private String windowType;
    // Group by {@link Metric#getTag(String)}
    private List<String> groupingKeyNames;
    private AggregatorFunctionType aggregatorFunctionType;
    // Query from {@link Metric#getMetric(String)}
    private String aggregateFieldName;
    private LimitOperatorType limitOperatorType;
    private BigDecimal limit;

    public Long getWindowMillis() {
        return Time.minutes(this.windowMinutes).toMilliseconds();
    }
    public Long getWindowSlideMillis() {
        return Time.minutes(this.windowSlideMinute).toMilliseconds();
    }

    /**
     * Evaluates this rule by comparing provided value with rules' limit based on limit operator
     * type.
     *
     * @param comparisonValue value to be compared with the limit
     */
    public boolean apply(BigDecimal comparisonValue) {
        switch (limitOperatorType) {
            case EQUAL:
                return comparisonValue.compareTo(limit) == 0;
            case NOT_EQUAL:
                return comparisonValue.compareTo(limit) != 0;
            case GREATER:
                return comparisonValue.compareTo(limit) > 0;
            case LESS:
                return comparisonValue.compareTo(limit) < 0;
            case LESS_EQUAL:
                return comparisonValue.compareTo(limit) <= 0;
            case GREATER_EQUAL:
                return comparisonValue.compareTo(limit) >= 0;
            default:
                throw new RuntimeException("Unknown limit operator type: " + limitOperatorType);
        }
    }

    /**
     * if the metric value can hit the rule
     * @param metric metric value
     * @return yes or not
     */
    public boolean isHit(Metric metric) {
            Preconditions.checkNotNull(metric, "metric must not be null");
            return Objects.equals(Rule.RuleState.ACTIVE, ruleState) && metric.getTags().keySet().containsAll(groupingKeyNames);
    }

    public enum AggregatorFunctionType {
        SUM(Boolean.FALSE),
        AVG(Boolean.TRUE),
        MIN(Boolean.FALSE),
        MAX(Boolean.FALSE);
        private final boolean needAllElements;

        AggregatorFunctionType(boolean needAllElements) {
            this.needAllElements = needAllElements;
        }

        public boolean noNeedAllElements() {
            return !isNeedAllElements();
        }
        public boolean isNeedAllElements() {
            return needAllElements;
        }

    }

    public enum LimitOperatorType {
        EQUAL("="),
        NOT_EQUAL("!="), GREATER_EQUAL(">="),
        LESS_EQUAL("<="), GREATER(">"),
        LESS("<");
        String operator;

        LimitOperatorType(String operator) {
            this.operator = operator;
        }

        public static LimitOperatorType fromString(String text) {
            for (LimitOperatorType b : LimitOperatorType.values()) {
                if (b.operator.equals(text)) {
                    return b;
                }

            }
            return null;
        }
    }

    public enum RuleState {
        ACTIVE,
        PAUSE,
        DELETE
    }
}