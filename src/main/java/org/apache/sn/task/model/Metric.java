package org.apache.sn.task.model;

import lombok.Data;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.util.Map;

@Data
public class Metric {
    private Map<String, String> tags = Maps.newHashMap();
    private Map<String, BigDecimal> metrics = Maps.newHashMap();
    private long eventTime;

    public String getTag(String name) {
        return tags.get(name);
    }
    public BigDecimal getMetric(String name) {
        return metrics.get(name);
    }
}