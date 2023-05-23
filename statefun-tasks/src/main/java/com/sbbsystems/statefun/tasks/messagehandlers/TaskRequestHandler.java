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

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.graph.PipelineGraphBuilder;
import com.sbbsystems.statefun.tasks.pipeline.PipelineHandler;
import com.sbbsystems.statefun.tasks.serialization.TaskRequestSerializer;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public final class TaskRequestHandler extends MessageHandler<TaskRequest, PipelineFunctionState> {
    private static final Logger LOG = LoggerFactory.getLogger(TaskRequestHandler.class);

    private static final List<TaskStatus.Status> ALLOWED_INITIAL_STATES = List.of(
            TaskStatus.Status.PENDING,
            TaskStatus.Status.COMPLETED,
            TaskStatus.Status.FAILED,
            TaskStatus.Status.CANCELLED
    );

    private final PipelineConfiguration configuration;

    public static TaskRequestHandler from(PipelineConfiguration configuration) {
        return new TaskRequestHandler(configuration);
    }

    private TaskRequestHandler(PipelineConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public boolean canHandle(Context context, Object input, PipelineFunctionState state) {
        return MessageTypes.isType(input, TaskRequest.class);
    }

    @Override
    public CheckedFunction<ByteString, TaskRequest, InvalidProtocolBufferException> getMessageBuilder() {
        return TaskRequest::parseFrom;
    }

    @Override
    public void handleMessage(Context context, TaskRequest taskRequest, PipelineFunctionState state)
            throws StatefunTasksException {

        try {

            if (!ALLOWED_INITIAL_STATES.contains(state.getStatus())) {
                throw new StatefunTasksException("Pipelines must have finished before they can be re-run");
            }

            // todo throw if pipeline is already active
            // reset pipeline state
            state.reset();

            // if inline then copy taskState to pipeline initial state
            var taskState = taskRequest.getState();

            var taskArgsAndKwargs = TaskRequestSerializer
                    .from(taskRequest)
                    .getArgsAndKwargs();

            var pipelineProto = Pipeline.parseFrom(taskArgsAndKwargs.getArg(0).getValue());
            var argsAndKwargs = taskArgsAndKwargs.slice(1);

            if (argsAndKwargs.getArgs().getItemsCount() > 0) {
                // if we have more args after pipeline in argsAndKwargs then pass to pipeline initial args
            }

            if (argsAndKwargs.getKwargs().getItemsCount() > 0) {
                // if we have kwargs in argsAndKwargs then pass to pipeline initial kwargs
            }

            // create the graph
            var graph = PipelineGraphBuilder
                    .from(state)
                    .fromProto(pipelineProto)
                    .build();

            // save graph structure to state
            graph.saveState();

            // create and start pipeline
            var pipelineHandler = PipelineHandler.from(configuration, state, graph);
            pipelineHandler.beginPipeline(context, taskRequest);
        }
        catch (InvalidProtocolBufferException e) {
            var ex = new InvalidMessageTypeException("Expected a TaskRequest containing a Pipeline", e);
            emit(context, taskRequest, MessageTypes.toTaskException(taskRequest, ex));
            throw ex;
        }
        catch (StatefunTasksException e) {
            emit(context, taskRequest, MessageTypes.toTaskException(taskRequest, e));
            throw e;
        }
    }

    private void emit(@NotNull Context context, TaskRequest taskRequest, Message message) {
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
