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

import com.google.common.base.Strings;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.events.PipelineEvents;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.graph.PipelineGraph;
import com.sbbsystems.statefun.tasks.serialization.TaskEntrySerializer;
import com.sbbsystems.statefun.tasks.serialization.TaskRequestSerializer;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.types.PipelineCancelledException;
import com.sbbsystems.statefun.tasks.util.Id;
import org.apache.flink.statefun.sdk.Context;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

import static java.util.Objects.*;


public class PipelineHandler {

    protected final PipelineConfiguration configuration;
    protected final PipelineFunctionState state;
    protected final PipelineGraph graph;
    protected final PipelineEvents events;

    public static PipelineHandler from(@NotNull PipelineConfiguration configuration,
                                       @NotNull PipelineFunctionState state,
                                       @NotNull PipelineGraph graph,
                                       @NotNull PipelineEvents events) {

        return new PipelineHandler(
                requireNonNull(configuration),
                requireNonNull(state),
                requireNonNull(graph),
                requireNonNull(events));
    }

    protected PipelineHandler(PipelineConfiguration configuration,
                            PipelineFunctionState state,
                            PipelineGraph graph,
                            PipelineEvents events) {

        this.configuration = configuration;
        this.state = state;
        this.graph = graph;
        this.events = events;
    }

    public TaskStatus.Status getStatus() {
        return state.getStatus();
    }

    public void cancel(Context context)
        throws StatefunTasksException {

        var status = state.getStatus();
        var validState = status == TaskStatus.Status.RUNNING || status == TaskStatus.Status.PENDING || status == TaskStatus.Status.PAUSED;

        if (!validState) {
            throw new StatefunTasksException("Pipeline is not in a state that can be cancelled");
        }

        state.setStatus(TaskStatus.Status.CANCELLING);
        events.notifyPipelineStatusChanged(context, TaskStatus.Status.CANCELLING);

        // request child pipelines to cancel
        for (var pipeline : state.getChildPipelines().view()) {
            var pauseRequest = TaskActionRequest.newBuilder()
                    .setId(Id.generate())
                    .setUid(Id.generate())
                    .setAction(TaskAction.CANCEL_PIPELINE);

            var functionType = MessageTypes.toFunctionType(pipeline.getAddress());
            context.send(functionType, pipeline.getId(), MessageTypes.wrap(pauseRequest.build()));
        }

        // create cancellation exception
        var taskException = createCancellationException();
        var callbackFunctionId = new StringBuilder(context.self().id()).reverse().toString(); // todo make this better
        var finallyTask = graph.getFinally();

        if (!isNull(finallyTask)) {
            // if we have a finally task then we need to submit it if it has not been already

            if (isNull(state.getResponseBeforeFinally())) {
                var entry = graph.getTaskEntry(finallyTask.getId());
                var taskEntry = TaskEntrySerializer.of(entry);
                var taskRequest = TaskRequestSerializer.of(state.getTaskRequest());
                var outgoingTaskRequest = taskRequest.createOutgoingTaskRequest(state, entry);

                outgoingTaskRequest
                        .setReplyAddress(MessageTypes.getCallbackFunctionAddress(configuration, callbackFunctionId))
                        .setRequest(taskEntry.mergeWith(MessageTypes.argsOfEmptyArray()))
                        .setState(requireNonNullElse(state.getCurrentTaskState(), Any.getDefaultInstance()));

                // submit the task
                context.send(MessageTypes.getSdkAddress(entry), MessageTypes.wrap(outgoingTaskRequest.build()));
            }

            // overwrite the result before finally with the cancellation exception
            state.setResponseBeforeFinally(TaskResultOrException.newBuilder().setTaskException(taskException).build());

        } else {
            // otherwise respond to pipeline's callback function with a cancellation exception
            taskException = taskException.toBuilder()
                    .setInvocationId(state.getInvocationId())
                    .setUid(state.getPipelineAddress().getId())
                    .build();

            var callbackAddress = MessageTypes.getCallbackFunctionAddress(configuration, callbackFunctionId);
            context.send(MessageTypes.toSdkAddress(callbackAddress), MessageTypes.wrap(taskException));
        }
    }

    public void pause(Context context)
        throws StatefunTasksException {

        pauseOrResume(context, true);
    }

    public void resume(Context context)
        throws StatefunTasksException {

        pauseOrResume(context, false);

        TaskSubmitter.unpauseTasks(context, state);
    }

    private TaskException createCancellationException() {
        try {
            throw new PipelineCancelledException("Pipeline was cancelled");
        } catch (PipelineCancelledException e) {
            return MessageTypes.toTaskException(state.getTaskRequest(), e, state.getCurrentTaskState());
        }
    }

    private void pauseOrResume(Context context, boolean pause)
            throws StatefunTasksException {

        var status = state.getStatus();
        var newStatus = pause ? TaskStatus.Status.PAUSED : TaskStatus.Status.RUNNING;
        var validState = status == TaskStatus.Status.RUNNING || status == TaskStatus.Status.PENDING || status == TaskStatus.Status.PAUSED;

        if (!validState) {
            throw new StatefunTasksException("Pipeline is not in a state that can be paused / un-paused");
        }

        state.setStatus(newStatus);
        events.notifyPipelineStatusChanged(context, newStatus);

        // request child pipelines to pause / un-pause
        for (var pipeline : state.getChildPipelines().view()) {
            var pauseRequest = TaskActionRequest.newBuilder()
                    .setId(Id.generate())
                    .setUid(Id.generate())
                    .setAction(pause ? TaskAction.PAUSE_PIPELINE: TaskAction.UNPAUSE_PIPELINE);

            var functionType = MessageTypes.toFunctionType(pipeline.getAddress());
            context.send(functionType, pipeline.getId(), MessageTypes.wrap(pauseRequest.build()));
        }
    }

    protected boolean notInThisInvocation(TaskResultOrException message) {
        if (message.hasTaskResult()) {
            return !message.getTaskResult().getInvocationId().equals(state.getInvocationId());
        }

        if (message.hasTaskException()) {
            return !message.getTaskException().getInvocationId().equals(state.getInvocationId());
        }

        return true;
    }

    protected void respondWithResult(@NotNull Context context, TaskRequest taskRequest, TaskResult taskResult) {
        var result = state.getIsFruitful()
                ? taskResult.getResult()
                : MessageTypes.tupleOfEmptyArray();

        var outgoingTaskResult = state.getIsInline()
                ? MessageTypes.toOutgoingTaskResult(taskRequest, result, taskResult.getState())
                : MessageTypes.toOutgoingTaskResult(taskRequest, result);

        state.setTaskResult(outgoingTaskResult);
        state.setStatus(TaskStatus.Status.COMPLETED);
        events.notifyPipelineStatusChanged(context, TaskStatus.Status.COMPLETED);
        respond(context, taskRequest, outgoingTaskResult);
    }

    protected void respondWithError(@NotNull Context context, TaskRequest taskRequest, TaskException error) {
        var outgoingTaskException = state.getIsInline()
                ? MessageTypes.toOutgoingTaskException(taskRequest, error, error.getState())
                : MessageTypes.toOutgoingTaskException(taskRequest, error);

        state.setTaskException(outgoingTaskException);
        state.setStatus(TaskStatus.Status.FAILED);
        events.notifyPipelineStatusChanged(context, TaskStatus.Status.FAILED);
        respond(context, taskRequest, outgoingTaskException);
    }

    protected void respond(@NotNull Context context, TaskRequest taskRequest, Message message) {
        if (!Strings.isNullOrEmpty(taskRequest.getReplyTopic())) {
            // send a message to egress if reply_topic was specified
            context.send(MessageTypes.getEgress(configuration), MessageTypes.toEgress(message, taskRequest.getReplyTopic()));
        }
        else if (taskRequest.hasReplyAddress()) {
            // else call back to a particular flink function if reply_address was specified
            context.send(MessageTypes.toSdkAddress(taskRequest.getReplyAddress()), MessageTypes.wrap(message));
        }
        else if (!Objects.isNull(context.caller())) {
            // else call back to the caller of this function (if there is one)
            context.send(context.caller(), MessageTypes.wrap(message));
        }
    }
}
