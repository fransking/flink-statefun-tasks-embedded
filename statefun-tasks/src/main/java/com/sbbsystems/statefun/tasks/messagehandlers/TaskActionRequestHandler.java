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

package com.sbbsystems.statefun.tasks.messagehandlers;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.events.PipelineEvents;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;

public class TaskActionRequestHandler extends MessageHandler<TaskActionRequest, PipelineFunctionState> {
    private static final TypedValue PAUSE_PIPELINE_SIGNAL = MessageTypes.wrap(CallbackSignal.newBuilder()
            .setValue(CallbackSignal.Signal.PAUSE_PIPELINE)
            .build());
    private static final TypedValue RESUME_PIPELINE_SIGNAL = MessageTypes.wrap(CallbackSignal.newBuilder()
            .setValue(CallbackSignal.Signal.RESUME_PIPELINE)
            .build());
    private final FunctionType callbackFunctionType;
    private static final Logger LOG = LoggerFactory.getLogger(TaskActionRequestHandler.class);

    private TaskActionRequestHandler(FunctionType callbackFunctionType, PipelineConfiguration configuration) {
        super(configuration);
        this.callbackFunctionType = callbackFunctionType;
    }

    public static TaskActionRequestHandler of(FunctionType callbackFunctionType, PipelineConfiguration configuration) {
        return new TaskActionRequestHandler(callbackFunctionType, configuration);
    }

    @Override
    public boolean canHandle(Context context, Object input, PipelineFunctionState state) {
        return MessageTypes.isType(input, TaskActionRequest.class);
    }

    @Override
    public CheckedFunction<ByteString, TaskActionRequest, InvalidProtocolBufferException> getMessageBuilder() {
        return TaskActionRequest::parseFrom;
    }

    @Override
    public void handleMessage(Context context, TaskActionRequest taskActionRequest, PipelineFunctionState state)
            throws StatefunTasksException {

        var result = TaskActionResult.newBuilder()
                .setId(taskActionRequest.getId())
                .setUid(taskActionRequest.getUid())
                .setAction(taskActionRequest.getAction())
                .build();

        var events = PipelineEvents.from(configuration, state);

        LOG.info(MessageFormat.format("Received task action request {0}", taskActionRequest.getAction().name()));

        switch (taskActionRequest.getAction()) {
            case PAUSE_PIPELINE:
                if (canPause(state)) {
                    sendSignalToCallbackIncludingChildren(context, state, PAUSE_PIPELINE_SIGNAL);
                    respond(context, taskActionRequest, result);
                    state.setStatus(TaskStatus.Status.PAUSED);
                    events.notifyPipelineStatusChanged(context, TaskStatus.Status.PAUSED);
                } else {
                    var message = MessageFormat.format("Pipeline is not in a state that can be paused ({0})", state.getStatus().name());
                    respondWithError(context, taskActionRequest, message);
                }
                break;
            case UNPAUSE_PIPELINE:
                if (canPause(state)) {
                    sendSignalToCallbackIncludingChildren(context, state, RESUME_PIPELINE_SIGNAL);
                    respond(context, taskActionRequest, result);
                    state.setStatus(TaskStatus.Status.RUNNING);
                    events.notifyPipelineStatusChanged(context, TaskStatus.Status.RUNNING);
                } else {
                    var message = MessageFormat.format("Pipeline is not in a state that can be paused ({0})", state.getStatus().name());
                    respondWithError(context, taskActionRequest, message);
                }
                break;
            default:
                throw new StatefunTasksException("Unknown action: " + taskActionRequest.getAction().name());
        }
    }


    private void sendSignalToCallbackIncludingChildren(Context context, PipelineFunctionState state, TypedValue signal) {
        context.send(callbackFunctionType, context.self().id(), signal);
        for (var pipeline : state.getChildPipelines().view()) {
            context.send(callbackFunctionType, pipeline.getId(), signal);
        }
    }

    private boolean canPause(PipelineFunctionState state) {
        var status = state.getStatus();
        return status == TaskStatus.Status.RUNNING || status == TaskStatus.Status.PENDING || status == TaskStatus.Status.PAUSED;
    }

    @NotNull
    private static TaskActionException createTaskActionException(TaskActionRequest taskActionRequest, String exceptionMessage) {
        return TaskActionException
                .newBuilder()
                .setId(taskActionRequest.getId())
                .setUid(taskActionRequest.getUid())
                .setAction(taskActionRequest.getAction())
                .setExceptionMessage(exceptionMessage)
                .build();
    }

    private void respondWithError(Context context, TaskActionRequest taskActionRequest, String message) {
        LOG.info(MessageFormat.format("{0} - {1}", taskActionRequest.getId(), message));
        respond(context, taskActionRequest, createTaskActionException(taskActionRequest, message));
    }
}
