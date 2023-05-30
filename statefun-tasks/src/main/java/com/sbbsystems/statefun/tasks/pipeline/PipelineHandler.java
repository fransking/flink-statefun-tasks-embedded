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
package com.sbbsystems.statefun.tasks.pipeline;

import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.ArrayOfAny;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.graph.PipelineGraph;
import com.sbbsystems.statefun.tasks.serialization.TaskEntrySerializer;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import com.sbbsystems.statefun.tasks.util.Id;
import org.apache.flink.statefun.sdk.Context;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.stream.Collectors;


public final class PipelineHandler {
    private final PipelineConfiguration configuration;
    private final PipelineFunctionState state;
    private final PipelineGraph graph;

    public static PipelineHandler from(@NotNull PipelineConfiguration configuration,
                                       @NotNull PipelineFunctionState state,
                                       @NotNull PipelineGraph graph) {
        return new PipelineHandler(
                Objects.requireNonNull(configuration),
                Objects.requireNonNull(state),
                Objects.requireNonNull(graph));
    }

    private PipelineHandler(PipelineConfiguration configuration, PipelineFunctionState state, PipelineGraph graph) {
        this.configuration = configuration;
        this.state = state;
        this.graph = graph;
    }

    public void beginPipeline(Context context, TaskRequest incomingTaskRequest)
        throws StatefunTasksException {

        state.setTaskRequest(incomingTaskRequest);
        state.setInvocationId(Id.generate());
        state.setIsFruitful(incomingTaskRequest.getIsFruitful()); // todo move this into the calling task
        state.setStatus(TaskStatus.Status.RUNNING);

        if (state.getIsInline()) {
            // if this is an inline pipeline then copy incomingTaskRequest state to pipeline initial state
            state.setInitialState(incomingTaskRequest.getState());
        }

        // set latest state of pipeline equal to initial state
        state.setCurrentTaskState(state.getInitialState());

        var entry = graph.getHead();

        if (Objects.isNull(entry)) {
            throw new StatefunTasksException("Cannot run an empty pipeline");
        }

        // get the initial tasks to call and the args and kwargs
        var initialTasks = graph.getInitialTasks(entry).collect(Collectors.toUnmodifiableList());
        var initialArgsAndKwargs = state.getInitialArgsAndKwargs();

        // we may have no initial tasks in the case of empty groups so continue to iterate over these
        // note that implicitly, the result of an empty group is [] so we update initialArgsAndKwargs accordingly
        while (initialTasks.isEmpty() && !Objects.isNull(entry)) {
            initialArgsAndKwargs = MessageTypes.argsOfEmptyArray();

            entry = graph.getNextEntry(entry);
            initialTasks = graph.getInitialTasks(entry).collect(Collectors.toUnmodifiableList());
        }

        if (initialTasks.isEmpty()) {
            // if we have a completely empty pipeline after iterating over empty groups then return empty result
            state.setStatus(TaskStatus.Status.COMPLETED);
            var taskResult = MessageTypes.toTaskResult(incomingTaskRequest, ArrayOfAny.getDefaultInstance());
            context.send(MessageTypes.getEgress(configuration), MessageTypes.toEgress(taskResult, incomingTaskRequest.getReplyTopic()));
        }
        else {
            // else call the initial tasks
            for (var task: initialTasks) {
                var taskEntry = graph.getTaskEntry(task.getId());

                var taskArgsAndKwargs = TaskEntrySerializer.of(taskEntry).mergeWith(initialArgsAndKwargs);

                var outgoingTaskRequest = TaskRequest.newBuilder()
                        .setId(taskEntry.taskId)
                        .setUid(taskEntry.uid)
                        .setType(taskEntry.taskType)
                        .setInvocationId(state.getInvocationId())
                        .setRequest(taskArgsAndKwargs);

                // add state if present
                if (!Objects.isNull(state.getInitialState())) {
                    outgoingTaskRequest.setState(state.getInitialState());
                }

                // add meta properties
                setMetaData(context, incomingTaskRequest, taskEntry, outgoingTaskRequest);

                // send message
                context.send(MessageTypes.toSdkAddress(taskEntry), MessageTypes.wrap(outgoingTaskRequest.build()));
            }

            // todo remove once TaskResult processing is working
            var taskResult = MessageTypes.toTaskResult(incomingTaskRequest, StringValue.of("DONE"));
            context.send(MessageTypes.getEgress(configuration), MessageTypes.toEgress(taskResult, incomingTaskRequest.getReplyTopic()));
        }
    }

    private void setMetaData(Context context, TaskRequest incomingTaskRequest, TaskEntry taskEntry, TaskRequest.Builder outgoingTaskRequest) {
        var pipelineAddress = MessageTypes.toTypeName(context.self());
        var pipelineId = context.self().id();
        outgoingTaskRequest.putMeta("pipeline_address", pipelineAddress);
        outgoingTaskRequest.putMeta("pipeline_id", pipelineId);

        var rootPipelineAddress =  incomingTaskRequest.getMetaOrDefault("root_pipeline_address", pipelineAddress);
        var rootPipelineId =  incomingTaskRequest.getMetaOrDefault("root_pipeline_id", pipelineId);
        outgoingTaskRequest.putMeta("root_pipeline_address", rootPipelineAddress);
        outgoingTaskRequest.putMeta("root_pipeline_id", rootPipelineId);

        if (!Objects.isNull(context.caller())) {
            var parentTaskAddress = MessageTypes.toTypeName(context.caller());
            var parentTaskId = context.caller().id();
            outgoingTaskRequest.putMeta("parent_task_address", parentTaskAddress);
            outgoingTaskRequest.putMeta("parent_task_id", parentTaskId);
        }

        if (state.getIsInline()) {
            var inlineParentPipelineAddress = incomingTaskRequest.getMetaOrDefault("inline_parent_pipeline_address", pipelineAddress);
            var inlineParentPipelineId = incomingTaskRequest.getMetaOrDefault("inline_parent_pipeline_id", pipelineId);
            outgoingTaskRequest.putMeta("inline_parent_pipeline_address", inlineParentPipelineAddress);
            outgoingTaskRequest.putMeta("inline_parent_pipeline_id", inlineParentPipelineId);
        }

        outgoingTaskRequest.putMeta("display_name", taskEntry.displayName);
    }
}
