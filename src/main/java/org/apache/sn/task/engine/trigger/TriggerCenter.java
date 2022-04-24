package org.apache.sn.task.engine.trigger;

import org.apache.flink.shaded.guava18.com.google.common.collect.ArrayListMultimap;
import org.apache.flink.shaded.guava18.com.google.common.collect.Multimap;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TriggerCenter {
    /**
     * key: trigger timestamp
     * value: trigger list
     */
    private final Multimap<Long, Trigger> triggerRegister = ArrayListMultimap.create();

    public TriggerCenter() {
        start();
    }

    public void register(Trigger trigger) {
        triggerRegister.put(trigger.getTimestamp(), trigger);
    }

    /**
     * check and trigger in every millisecond
     */
    public void start() {
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(
                        () -> triggerRegister.get(System.currentTimeMillis()).forEach(Trigger::trigger),
                        0,
                        1,
                        TimeUnit.MILLISECONDS
                );
    }
}
