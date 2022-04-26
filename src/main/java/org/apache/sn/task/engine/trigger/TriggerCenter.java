package org.apache.sn.task.engine.trigger;

import org.apache.commons.collections.MapUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.TreeMultimap;

import java.util.Collection;
import java.util.NavigableMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TriggerCenter {
    /**
     * key: trigger timestamp
     * value: trigger list
     */
    private final TreeMultimap<Long, Trigger> triggerRegister = TreeMultimap.create((o1, o2) -> (int) (o1 - o2), (t1, t2) -> (int) (t1.getTimestamp() - t2.getTimestamp()));

    public TriggerCenter() {
        start();
    }

    public void register(Trigger trigger) {
        if (trigger.getTimestamp() > System.currentTimeMillis()) {
            triggerRegister.put(trigger.getTimestamp(), trigger);
        }
    }

    /**
     * check and trigger in every millisecond
     */
    public void start() {
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(
                        () -> {
                            try {
                                //get all triggers whoes time is up
                                NavigableMap<Long, Collection<Trigger>> triggerMap = triggerRegister.asMap().headMap(System.currentTimeMillis(), Boolean.TRUE);
                                if (MapUtils.isNotEmpty(triggerMap)) {
                                    //trigger all
                                    triggerMap.values().stream().parallel()
                                            .flatMap(Collection::stream)
                                            .forEach(Trigger::trigger);
                                    //remove these triggers
                                    triggerMap.keySet()
                                            .forEach(triggerRegister::removeAll);
                                }
                            } catch (Exception e) {
                                System.err.println(e);
                            }

                        },
                        0,
                        10,
                        TimeUnit.MILLISECONDS
                );
    }

}
