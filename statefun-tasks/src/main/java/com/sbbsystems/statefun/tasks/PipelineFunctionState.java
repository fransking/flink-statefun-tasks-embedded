/*
 * Copyright [2023] [Frans King, Luke Ashworth]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sbbsystems.statefun.tasks;

import com.google.protobuf.Any;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.ArgsAndKwargs;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.graph.MapOfEntries;
import com.sbbsystems.statefun.tasks.pipeline.GroupDeferredTasksState;
import com.sbbsystems.statefun.tasks.types.GroupEntry;
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.apache.flink.statefun.sdk.state.PersistedAppendingBuffer;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyList;

public final class PipelineFunctionState {
    // todo ensure configurable state expiry

    @Persisted
    private final PersistedTable<String, TaskEntry> taskEntries;
    @Persisted
    private final PersistedTable<String, GroupEntry> groupEntries;
    @Persisted
    private final PersistedValue<MapOfEntries> entries;
    @Persisted
    private final PersistedValue<String> head;
    @Persisted
    private final PersistedValue<String> tail;
    @Persisted
    private final PersistedValue<Boolean> isInline;
    @Persisted
    private final PersistedValue<Boolean> isFruitful;
    @Persisted
    private final PersistedValue<ArgsAndKwargs> initialArgsAndKwargs;
    @Persisted
    private final PersistedValue<Any> initialState;
    @Persisted
    private final PersistedValue<Any> currentTaskState;
    @Persisted
    private final PersistedValue<String> invocationId;
    @Persisted
    private final PersistedValue<TaskRequest> taskRequest;
    @Persisted
    private final PersistedValue<Address> callerAddress;
    @Persisted
    private final PersistedValue<Address> rootPipelineAddress;
    @Persisted
    private final PersistedValue<Integer> status;
    @Persisted
    private final PersistedTable<String, GroupDeferredTasksState> deferredTasks;
    @Persisted
    private final PersistedTable<String, TaskResultOrException> intermediateGroupResults;

    private MapOfEntries cachedEntries = null;
    private final Map<String, GroupDeferredTasksState> cachedDeferredTasks = new HashMap<>();

    public static PipelineFunctionState newInstance() {
        return new PipelineFunctionState(Expiration.none());
    }

    public static PipelineFunctionState withExpiration(Expiration expiration) {
        return new PipelineFunctionState(expiration);
    }

    private PipelineFunctionState(Expiration expiration) {
        taskEntries = PersistedTable.of("taskEntries", String.class, TaskEntry.class, expiration);
        groupEntries = PersistedTable.of("groupEntries", String.class, GroupEntry.class, expiration);
        entries = PersistedValue.of("entries", MapOfEntries.class, expiration);
        head = PersistedValue.of("head", String.class, expiration);
        tail = PersistedValue.of("tail", String.class, expiration);
        isInline = PersistedValue.of("isInline", Boolean.class, expiration);
        isFruitful = PersistedValue.of("isFruitful", Boolean.class, expiration);
        initialArgsAndKwargs = PersistedValue.of("initialArgsAndKwargs", ArgsAndKwargs.class, expiration);
        initialState = PersistedValue.of("initialState", Any.class, expiration);
        currentTaskState = PersistedValue.of("currentTaskState", Any.class, expiration);
        invocationId = PersistedValue.of("invocationId", String.class, expiration);
        taskRequest = PersistedValue.of("taskRequest", TaskRequest.class, expiration);
        callerAddress = PersistedValue.of("callerAddress", Address.class, expiration);
        rootPipelineAddress = PersistedValue.of("rootPipelineAddress", Address.class, expiration);
        status = PersistedValue.of("status", Integer.class, expiration);
        deferredTasks = PersistedTable.of("deferredTasksMap", String.class, GroupDeferredTasksState.class, expiration);
        intermediateGroupResults = PersistedTable.of("intermediateGroupResults", String.class, TaskResultOrException.class, expiration);
    }

    public PersistedTable<String, TaskEntry> getTaskEntries() {
        return taskEntries;
    }

    public PersistedTable<String, GroupEntry> getGroupEntries() {
        return groupEntries;
    }

    public MapOfEntries getEntries() {
        if (!Objects.isNull(cachedEntries)) {
            return cachedEntries;
        }

        cachedEntries = entries.getOrDefault(new MapOfEntries());
        return cachedEntries;
    }

    public void setEntries(MapOfEntries tasks) {
        cachedEntries = tasks;
        this.entries.set(cachedEntries);
    }

    @NotNull
    public GroupDeferredTasksState getDeferredTasks(String groupId) {
        var groupCachedTasks = cachedDeferredTasks.getOrDefault(groupId, null);
        if (groupCachedTasks != null) {
            return groupCachedTasks;
        }
        var groupTasks = deferredTasks.get(groupId);
        if (groupTasks == null) {
            groupTasks = GroupDeferredTasksState.of(emptyList());
        }
        cachedDeferredTasks.put(groupId, groupTasks);
        return groupTasks;
    }

    public void cacheDeferredTasks(String groupId, GroupDeferredTasksState deferredTasks) {
        cachedDeferredTasks.put(groupId, deferredTasks);
    }

    public void saveDeferredTasks() {
        cachedDeferredTasks.forEach(deferredTasks::set);
    }

    public PersistedTable<String, TaskResultOrException> getIntermediateGroupResults() {
        return intermediateGroupResults;
    }

    public String getHead() {
        return head.get();
    }

    public void setHead(String head) {
        this.head.set(head);
    }

    public String getTail() {
        return tail.get();
    }

    public void setTail(String tail) {
        this.tail.set(tail);
    }

    public boolean getIsInline() {
        return isInline.getOrDefault(false);
    }

    public void setIsInline(boolean isInline) {
        this.isInline.set(isInline);
    }

    public boolean getIsFruitful() {
        return isFruitful.getOrDefault(true);
    }

    public void setIsFruitful(boolean isFruitful) {
        this.isFruitful.set(isFruitful);
    }

    public ArgsAndKwargs getInitialArgsAndKwargs() {
        return this.initialArgsAndKwargs.get();
    }

    public void setInitialArgsAndKwargs(ArgsAndKwargs initialArgsAndKwargs) {
        this.initialArgsAndKwargs.set(initialArgsAndKwargs);
    }

    public Any getInitialState() {
        return initialState.get();
    }

    public void setInitialState(Any initialState) {
        this.initialState.set(initialState);
    }

    public void setCurrentTaskState(Any currentTaskState) {
        this.currentTaskState.set(currentTaskState);
    }

    public Any getCurrentTaskState() {
        return currentTaskState.get();
    }

    public String getInvocationId() {
        return invocationId.get();
    }

    public void setInvocationId(String invocationId) {
        this.invocationId.set(invocationId);
    }

    public TaskRequest getTaskRequest() {
        return taskRequest.get();
    }

    public void setTaskRequest(TaskRequest taskRequest) {
        this.taskRequest.set(taskRequest);
    }

    public TaskStatus.Status getStatus() {
        var status = this.status.getOrDefault(0);
        return TaskStatus.Status.forNumber(status);
    }

    public void setStatus(TaskStatus.Status status) {
        this.status.set(status.getNumber());
    }

    public void reset()
        throws StatefunTasksException {

        try {
            for (var field : PipelineFunctionState.class.getDeclaredFields()) {
                if (field.getType().isAssignableFrom(PersistedValue.class)) {
                    ((PersistedValue<?>) field.get(this)).clear();
                } else if (field.getType().isAssignableFrom(PersistedTable.class)) {
                    ((PersistedTable<?, ?>) field.get(this)).clear();
                } else if (field.getType().isAssignableFrom(PersistedAppendingBuffer.class)) {
                    ((PersistedAppendingBuffer<?>) field.get(this)).clear();
                }
            }
        } catch (IllegalAccessException e) {
            throw new StatefunTasksException("Failed to reset pipeline function state", e);
        }
    }
}
