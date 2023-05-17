package com.sbbsystems.statefun.tasks;

import com.sbbsystems.statefun.tasks.types.GroupEntry;
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedTable;

public class PipelineFunctionState {
    @Persisted
    private final PersistedTable<String, TaskEntry> taskEntries = PersistedTable.of("taskEntries", String.class, TaskEntry.class);

    @Persisted
    private final PersistedTable<String, GroupEntry> groupEntries = PersistedTable.of("groupEntries", String.class, GroupEntry.class);

    public static PipelineFunctionState getInstance() {
        return new PipelineFunctionState();
    }

    private PipelineFunctionState() {
    }

    public PersistedTable<String, TaskEntry> getTaskEntries() {
        return taskEntries;
    }

    public PersistedTable<String, GroupEntry> getGroupEntries() {
        return groupEntries;
    }
}
