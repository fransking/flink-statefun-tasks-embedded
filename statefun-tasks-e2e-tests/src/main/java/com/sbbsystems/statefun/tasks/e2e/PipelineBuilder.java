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
package com.sbbsystems.statefun.tasks.e2e;

import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.util.Id;

import java.util.Objects;

import static com.sbbsystems.statefun.tasks.types.MessageTypes.packAny;

public class PipelineBuilder {

    private final Pipeline.Builder pipeline;

    public static PipelineBuilder inParallel(Iterable<Pipeline> entries) {
        return inParallel(entries, false);
    }

    public static PipelineBuilder inParallel(Iterable<Pipeline> entries, boolean returnExceptions) {
        return new PipelineBuilder().addGroup(entries, returnExceptions);
    }

    public static PipelineBuilder beginWith(String taskType) {
        return new PipelineBuilder().addTask(taskType, null, false, false);
    }

    public static PipelineBuilder beginWith(String taskType, Message request) {
        return new PipelineBuilder().addTask(taskType, request, false, false);
    }

    private PipelineBuilder() {
        pipeline = Pipeline.newBuilder();
    }

    public Pipeline build() {
        return pipeline.build();
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

    public PipelineBuilder inline() {
        pipeline.setInline(true);
        return this;
    }

    public PipelineBuilder continueWith(String taskType) {
        return continueWith(taskType, null);
    }

    public PipelineBuilder continueWith(String taskType, Message request) {
        return this.addTask(taskType, request, false, false);
    }

    public PipelineBuilder exceptionally(String taskType) {
        return exceptionally(taskType, null);
    }

    public PipelineBuilder exceptionally(String taskType, Message request) {
        return this.addTask(taskType, request, true, false);
    }

    public PipelineBuilder finally_do(String taskType) {
        return finally_do(taskType, null);
    }

    public PipelineBuilder finally_do(String taskType, Message request) {
        return this.addTask(taskType, request, false, true);
    }

    private PipelineBuilder addTask(String taskType, Message request, boolean isExceptionally, boolean isFinally) {
        var taskEntry = TaskEntry.newBuilder()
                .setNamespace(EndToEndRemoteFunction.FUNCTION_TYPE.namespace())
                .setWorkerName(EndToEndRemoteFunction.FUNCTION_TYPE.name())
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

    private PipelineBuilder addGroup(Iterable<Pipeline> entries, boolean returnExceptions) {
        var groupEntry = GroupEntry
                .newBuilder()
                .setGroupId(Id.generate())
                .setReturnExceptions(returnExceptions);

        for (var entry: entries) {
            groupEntry.addGroup(entry);
        }

        var pipelineEntry = PipelineEntry.newBuilder()
                .setGroupEntry(groupEntry)
                .build();
        pipeline.addEntries(pipelineEntry);

        return this;
    }
}
