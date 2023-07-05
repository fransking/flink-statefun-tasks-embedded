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
import com.sbbsystems.statefun.tasks.generated.TaskActionRequest;
import com.sbbsystems.statefun.tasks.generated.TaskActionResult;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.graph.PipelineGraphBuilder;
import com.sbbsystems.statefun.tasks.pipeline.PipelineHandler;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;

import static com.sbbsystems.statefun.tasks.types.MessageTypes.packAny;
import static java.util.Objects.isNull;

public class TaskActionRequestHandler extends MessageHandler<TaskActionRequest, PipelineFunctionState> {
    private static final Logger LOG = LoggerFactory.getLogger(TaskActionRequestHandler.class);

    public static TaskActionRequestHandler of(PipelineConfiguration configuration) {
        return new TaskActionRequestHandler(configuration);
    }

    private TaskActionRequestHandler(PipelineConfiguration configuration) {
        super(configuration);
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

        LOG.info(MessageFormat.format("Received task action request {0}", taskActionRequest.getAction().name()));

        var result = TaskActionResult.newBuilder()
                .setId(taskActionRequest.getId())
                .setUid(taskActionRequest.getUid())
                .setAction(taskActionRequest.getAction());

        try {
            var graph = PipelineGraphBuilder.from(state).build();
            var events = PipelineEvents.from(configuration, state);
            var pipeline = PipelineHandler.from(configuration, state, graph, events);

            switch (taskActionRequest.getAction()) {
                case PAUSE_PIPELINE:
                    pipeline.pause(context);
                    respond(context, taskActionRequest, result.build());
                    break;

                case UNPAUSE_PIPELINE:
                    pipeline.resume(context);
                    respond(context, taskActionRequest, result.build());
                    break;

                case CANCEL_PIPELINE:
                    pipeline.cancel(context);
                    respond(context, taskActionRequest, result.build());
                    break;

                case GET_STATUS:
                    result.setResult(packAny(TaskStatus.newBuilder().setValue(pipeline.getStatus())));
                    respond(context, taskActionRequest, result.build());
                    break;

                case GET_REQUEST:
                    if (isNull(state.getTaskRequest())) {
                        throw new StatefunTasksException("Task request not found");
                    }
                    result.setResult(packAny(state.getTaskRequest()));
                    respond(context, taskActionRequest, result.build());
                    break;

                case GET_RESULT:
                    if (!isNull(state.getTaskResult())) {
                        result.setResult(packAny(state.getTaskResult()));
                    } else if (!isNull(state.getTaskException())) {
                        result.setResult(packAny(state.getTaskException()));
                    } else {
                        throw new StatefunTasksException("Task result not found");
                    }
                    respond(context, taskActionRequest, result.build());
                    break;

                default:
                    throw new StatefunTasksException("Unknown action: " + taskActionRequest.getAction().name());
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            respond(context, taskActionRequest, MessageTypes.toTaskActionException(taskActionRequest, e));
        }
    }
}
