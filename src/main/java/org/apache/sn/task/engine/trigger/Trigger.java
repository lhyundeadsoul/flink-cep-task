package org.apache.sn.task.engine.trigger;

import org.apache.sn.task.engine.window.Window;

import java.util.Objects;

public class Trigger {
    private final Window window;

    public Trigger(Window window) {
        this.window = window;
    }

    public void trigger(){
        window.result();
    }

    public Long getTimestamp() {
        return window.getEndTimestamp();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Trigger trigger = (Trigger) o;
        return window.equals(trigger.window);
    }

    @Override
    public int hashCode() {
        return Objects.hash(window);
    }
}
