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
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.graph.DeferredTaskIds;
import com.sbbsystems.statefun.tasks.graph.v2.MapOfGraphEntries;
import com.sbbsystems.statefun.tasks.types.DeferredTask;
import com.sbbsystems.statefun.tasks.types.GroupEntry;
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.*;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.isNull;

public final class PipelineFunctionState {
    @Persisted
    private final Expiration expiration;
    @Persisted
    private final PersistedTable<String, TaskEntry> taskEntries;
    @Persisted
    private final PersistedTable<String, GroupEntry> groupEntries;
    @Persisted
    private final PersistedValue<MapOfGraphEntries> graphEntries;
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
    private Any cachedCurrentTaskState;
    @Persisted
    private final PersistedValue<String> invocationId;
    @Persisted
    private final PersistedValue<TaskRequest> taskRequest;
    @Persisted
    private final PersistedValue<TaskResult> taskResult;
    @Persisted
    private final PersistedValue<TaskException> taskException;
    @Persisted
    private final PersistedValue<Address> callerAddress;
    @Persisted
    private final PersistedValue<Address> pipelineAddress;
    @Persisted
    private final PersistedValue<Address> rootPipelineAddress;
    @Persisted
    private final PersistedValue<Integer> status;
    @Persisted
    private final PersistedTable<String, TaskResultOrException> intermediateGroupResults;
    @Persisted
    private final PersistedValue<TaskResultOrException> responseBeforeFinally;
    @Persisted
    private final PersistedTable<String, DeferredTaskIds> deferredTaskIds;
    @Persisted
    private final PersistedTable<String, DeferredTask> deferredTasks;
    @Persisted
    private final PersistedAppendingBuffer<ChildPipeline> childPipelines;
    @Persisted
    private final PersistedAppendingBuffer<PausedTask> pausedTasks;
    @Persisted
    private final PersistedStateRegistry dynamicStateRegistry = new PersistedStateRegistry();
    @Persisted
    private final Map<String, PersistedAppendingBuffer<TaskResultOrException>> unOrderedIntermediateGroupResults = new HashMap<>();

    public static PipelineFunctionState newInstance() {
        return new PipelineFunctionState(Expiration.none());
    }

    public static PipelineFunctionState withExpiration(Expiration expiration) {
        return new PipelineFunctionState(expiration);
    }

    private PipelineFunctionState(Expiration expiration) {
        this.expiration = expiration;
        taskEntries = PersistedTable.of("taskEntries", String.class, TaskEntry.class, expiration);
        groupEntries = PersistedTable.of("groupEntries", String.class, GroupEntry.class, expiration);
        graphEntries = PersistedValue.of("graphEntries", MapOfGraphEntries.class, expiration);
        head = PersistedValue.of("head", String.class, expiration);
        tail = PersistedValue.of("tail", String.class, expiration);
        isInline = PersistedValue.of("isInline", Boolean.class, expiration);
        isFruitful = PersistedValue.of("isFruitful", Boolean.class, expiration);
        initialArgsAndKwargs = PersistedValue.of("initialArgsAndKwargs", ArgsAndKwargs.class, expiration);
        initialState = PersistedValue.of("initialState", Any.class, expiration);
        currentTaskState = PersistedValue.of("currentTaskState", Any.class, expiration);
        invocationId = PersistedValue.of("invocationId", String.class, expiration);
        taskRequest = PersistedValue.of("taskRequest", TaskRequest.class, expiration);
        taskResult = PersistedValue.of("taskResult", TaskResult.class, expiration);
        taskException = PersistedValue.of("taskResult", TaskException.class, expiration);
        callerAddress = PersistedValue.of("callerAddress", Address.class, expiration);
        pipelineAddress = PersistedValue.of("pipelineAddress", Address.class, expiration);
        rootPipelineAddress = PersistedValue.of("rootPipelineAddress", Address.class, expiration);
        status = PersistedValue.of("status", Integer.class, expiration);
        intermediateGroupResults = PersistedTable.of("intermediateGroupResults", String.class, TaskResultOrException.class, expiration);
        responseBeforeFinally = PersistedValue.of("responseBeforeFinally", TaskResultOrException.class, expiration);
        deferredTaskIds = PersistedTable.of("deferredTaskIds", String.class, DeferredTaskIds.class, expiration);
        deferredTasks = PersistedTable.of("deferredTasks", String.class, DeferredTask.class, expiration);
        childPipelines = PersistedAppendingBuffer.of("childPipelines", ChildPipeline.class, expiration);
        pausedTasks = PersistedAppendingBuffer.of("pausedTasks", PausedTask.class, expiration);
    }

    public Expiration getExpiration() {
        return expiration;
    }

    public PersistedTable<String, TaskEntry> getTaskEntries() {
        return taskEntries;
    }

    public PersistedTable<String, GroupEntry> getGroupEntries() {
        return groupEntries;
    }

    public MapOfGraphEntries getGraphEntries() {
        return graphEntries.getOrDefault(new MapOfGraphEntries());
    }

    public void setGraphEntries(MapOfGraphEntries entries) {
        this.graphEntries.set(entries);
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
        cachedCurrentTaskState = currentTaskState;
    }

    public Any getCurrentTaskState() {
        if (isNull(cachedCurrentTaskState)) {
            cachedCurrentTaskState = currentTaskState.get();
        }

        return cachedCurrentTaskState;
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

    public void setTaskResult(TaskResult taskResult) {
        this.taskResult.set(taskResult);
    }

    public TaskResult getTaskResult() {
        return taskResult.get();
    }

    public void setTaskException(TaskException taskException) {
        this.taskException.set(taskException);
    }

    public TaskException getTaskException() {
        return taskException.get();
    }

    public Address getCallerAddress() {
        return callerAddress.get();
    }

    public void setCallerAddress(Address callerAddress) {
        this.callerAddress.set(callerAddress);
    }

    public Address getPipelineAddress() {
        return pipelineAddress.get();
    }

    public void setPipelineAddress(Address pipelineAddress) {
        this.pipelineAddress.set(pipelineAddress);
    }

    public Address getRootPipelineAddress() {
        return rootPipelineAddress.get();
    }

    public void setRootPipelineAddress(Address rootPipelineAddress) {
        this.rootPipelineAddress.set(rootPipelineAddress);
    }

    public TaskStatus.Status getStatus() {
        var status = this.status.getOrDefault(0);
        return TaskStatus.Status.forNumber(status);
    }

    public void setStatus(TaskStatus.Status status) {
        this.status.set(status.getNumber());
    }

    public TaskResultOrException getResponseBeforeFinally() {
        return responseBeforeFinally.get();
    }

    public void setResponseBeforeFinally(TaskResultOrException response) {
        responseBeforeFinally.set(response);
    }

    public PersistedTable<String, DeferredTaskIds> getDeferredTaskIds() {
        return deferredTaskIds;
    }

    public PersistedTable<String, DeferredTask> getDeferredTasks() {
        return deferredTasks;
    }

    public PersistedAppendingBuffer<ChildPipeline> getChildPipelines() {
        return childPipelines;
    }

    public PersistedAppendingBuffer<PausedTask> getPausedTasks() {
        return pausedTasks;
    }

    public PersistedStateRegistry getDynamicStateRegistry() {
        return dynamicStateRegistry;
    }

    public Map<String, PersistedAppendingBuffer<TaskResultOrException>> getUnOrderedIntermediateGroupResults() {
        return unOrderedIntermediateGroupResults;
    }

    public void saveUpdatedState() {
        if (!isNull(cachedCurrentTaskState)) {
            currentTaskState.set(cachedCurrentTaskState);
        }
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
