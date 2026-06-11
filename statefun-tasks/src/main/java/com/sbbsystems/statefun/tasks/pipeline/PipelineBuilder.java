/*
 * Copyright [2023] [Frans King, Luke Ashworth]
 * Copyright [2026] [Frans King]
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
package com.sbbsystems.statefun.tasks.pipeline;

import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.util.Id;

import java.util.Objects;

import static com.sbbsystems.statefun.tasks.types.MessageTypes.packAny;

public class PipelineBuilder {

    private final String namespace;
    private final String workerName;
    private final Pipeline.Builder pipeline;

    /**
     * Creates a PipelineBuilder bound to the given worker namespace and name.
     * All task entries added to this builder will use these values as their
     * namespace and workerName.
     */
    public static PipelineBuilder forWorker(String namespace, String workerName) {
        return new PipelineBuilder(namespace, workerName);
    }

    private PipelineBuilder(String namespace, String workerName) {
        this.namespace = namespace;
        this.workerName = workerName;
        this.pipeline = Pipeline.newBuilder();
    }

    public Pipeline build() {
        return pipeline.build();
    }

    public PipelineBuilder beginWith(String taskType) {
        return addTask(taskType, null, false, false);
    }

    public PipelineBuilder beginWith(String taskType, Message request) {
        return addTask(taskType, request, false, false);
    }

    public PipelineBuilder inParallel(Iterable<Pipeline> entries) {
        return inParallel(entries, false);
    }

    public PipelineBuilder inParallel(Iterable<Pipeline> entries, boolean returnExceptions) {
        return addGroup(entries, returnExceptions, 0);
    }

    public PipelineBuilder inParallel(Iterable<Pipeline> entries, int maxParallelism) {
        return addGroup(entries, false, maxParallelism);
    }

    public PipelineBuilder withInitialState(Message initialState) {
        pipeline.setInitialState(packAny(initialState));
        return this;
    }

    public PipelineBuilder withInitialArgs(Message initialArgs) {
        pipeline.setInitialArgs(packAny(initialArgs));
        return this;
    }

    public PipelineBuilder withInitialKwargs(MapOfStringToAny initialKwargs) {
        pipeline.setInitialKwargs(initialKwargs);
        return this;
    }

    public PipelineBuilder withInitialKwargs(MapOfStringToValue initialKwargs) {
        pipeline.setInitialValueKwargs(initialKwargs);
        return this;
    }

    public PipelineBuilder inline() {
        pipeline.setInline(true);
        return this;
    }

    public PipelineBuilder continueWith(String taskType) {
        return continueWith(taskType, null);
    }

    public PipelineBuilder continueWith(String taskType, Message request) {
        return addTask(taskType, request, false, false);
    }

    public PipelineBuilder continueWith(Pipeline pipeline) {
        return addFrom(pipeline);
    }

    public PipelineBuilder exceptionally(String taskType, Message request) {
        return addTask(taskType, request, true, false);
    }

    public PipelineBuilder finally_do(String taskType) {
        return finally_do(taskType, null);
    }

    public PipelineBuilder finally_do(String taskType, Message request) {
        return addTask(taskType, request, false, true);
    }

    private PipelineBuilder addTask(String taskType, Message request, boolean isExceptionally, boolean isFinally) {
        var taskEntry = TaskEntry.newBuilder()
                .setNamespace(namespace)
                .setWorkerName(workerName)
                .setTaskType(taskType)
                .setTaskId(Id.generate())
                .setUid(Id.generate())
                .setIsExceptionally(isExceptionally)
                .setIsFinally(isFinally);

        if (!Objects.isNull(request)) {
            taskEntry.setRequest(packAny(request));
        }

        var pipelineEntry = PipelineEntry.newBuilder()
                .setTaskEntry(taskEntry)
                .build();
        pipeline.addEntries(pipelineEntry);

        return this;
    }

    private PipelineBuilder addGroup(Iterable<Pipeline> entries, boolean returnExceptions, int maxParallelism) {
        var groupEntry = GroupEntry
                .newBuilder()
                .setGroupId(Id.generate())
                .setReturnExceptions(returnExceptions)
                .setMaxParallelism(maxParallelism);

        for (var entry : entries) {
            groupEntry.addGroup(entry);
        }

        var pipelineEntry = PipelineEntry.newBuilder()
                .setGroupEntry(groupEntry)
                .build();
        pipeline.addEntries(pipelineEntry);

        return this;
    }

    private PipelineBuilder addFrom(Pipeline thisPipeline) {
        pipeline.addAllEntries(thisPipeline.getEntriesList());
        return this;
    }
}

