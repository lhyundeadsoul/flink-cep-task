package org.apache.sn.task.engine.window;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.sn.task.model.Metric;
import org.apache.sn.task.model.Rule;

import java.util.List;

public class SlidingWindowAssigner<IN extends Metric> extends WindowAssigner<IN> {
    private long earliestTimestamp = Long.MIN_VALUE;
    private long latestTimestamp = Long.MAX_VALUE;

    public SlidingWindowAssigner(Rule rule) {
        super(rule, out);
    }

    @Override
    public List<Window> createWindowIfNecessary(IN in) {
        List<Window> newWindowList = null;
        //if never open a window
        if (earliestTimestamp == Long.MIN_VALUE) {
            earliestTimestamp = in.getEventTime() - rule.getWindowMillis();
            newWindowList = doCreateWindowList(earliestTimestamp, in.getEventTime(), rule.getWindowMillis(), rule.getWindowSlideMillis());
            latestTimestamp = newWindowList.get(newWindowList.size() - 1).getEndTimestamp();
        } else {
            //there is already some windows
            //TODO only consider that there is just one range of time now
            if (in.getEventTime()>latestTimestamp){
                newWindowList = doCreateWindowList(
                        latestTimestamp - rule.getWindowMillis() + rule.getWindowSlideMillis(),
                            in.getEventTime(),
                            rule.getWindowMillis(),
                            rule.getWindowSlideMillis()
                        );
                latestTimestamp = newWindowList.get(newWindowList.size()-1).getEndTimestamp();
            } else if (in.getEventTime()<earliestTimestamp){
                long newEarliest = in.getEventTime() + (in.getEventTime() - earliestTimestamp) % rule.getWindowSlideMillis();
                newWindowList = doCreateWindowList(
                        newEarliest,
                        earliestTimestamp - rule.getWindowSlideMillis(),
                        rule.getWindowMillis(),
                        rule.getWindowSlideMillis()
                );
                earliestTimestamp = newEarliest;
            }

        }
        return newWindowList;
    }

    private List<Window> doCreateWindowList(long begin, long endOfBegin, Long windowMillis, Long windowSlideMillis) {
        List<Window> newWindowList = Lists.newArrayList();
        for (long i = begin; i<=endOfBegin; i+=windowSlideMillis){
            newWindowList.add(new Window(i, i+windowMillis, rule.getAggregatorFunctionType(),this));
        }
        return newWindowList;
    }


}
