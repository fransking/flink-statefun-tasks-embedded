package com.sbbsystems.statefun.tasks.graph;

import org.jetbrains.annotations.UnmodifiableView;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class NextStep {
    private final Entry entry;
    private final List<Task> skippedTasks;

    public static NextStep of(Entry entry, List<Task> skippedTasks) {
        return new NextStep(entry, Collections.unmodifiableList(skippedTasks));
    }

    private NextStep(Entry entry, List<Task> skippedTasks) {
        this.entry = entry;
        this.skippedTasks = skippedTasks;
    }

    public boolean hasEntry() {
        return !Objects.isNull(entry);
    }

    public Entry getEntry() {
        return entry;
    }

    @UnmodifiableView
    public List<Task> getSkippedTasks() {
        return skippedTasks;
    }
}
