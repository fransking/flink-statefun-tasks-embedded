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
import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.graph.PipelineGraphBuilder;
import com.sbbsystems.statefun.tasks.pipeline.BeginPipelineHandler;
import com.sbbsystems.statefun.tasks.serialization.TaskRequestSerializer;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class TaskRequestHandler extends MessageHandler<TaskRequest, PipelineFunctionState> {
    private static final Logger LOG = LoggerFactory.getLogger(TaskRequestHandler.class);

    private static final List<TaskStatus.Status> ALLOWED_INITIAL_STATES = List.of(
            TaskStatus.Status.PENDING,
            TaskStatus.Status.COMPLETED,
            TaskStatus.Status.FAILED,
            TaskStatus.Status.CANCELLED
    );

    public static TaskRequestHandler from(PipelineConfiguration configuration) {
        return new TaskRequestHandler(configuration);
    }

    private TaskRequestHandler(PipelineConfiguration configuration) {
        super(configuration);
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
    public void handleMessage(Context context, TaskRequest taskRequest, PipelineFunctionState state) {

        try {

            if (!ALLOWED_INITIAL_STATES.contains(state.getStatus())) {
                throw new StatefunTasksException("Pipelines must have finished before they can be re-run");
            }

            // reset pipeline state
            state.reset();

            var taskArgsAndKwargs = TaskRequestSerializer.of(taskRequest).getArgsAndKwargsSerializer();
            var pipelineProto = Pipeline.parseFrom(taskArgsAndKwargs.getArg(0).getValue());

            state.setIsInline(pipelineProto.getInline());

            // set the args and kwargs for the initial tasks in the pipeline
            var initialArgsAndKwargs = taskArgsAndKwargs.getInitialArgsAndKwargs(pipelineProto, 1);
            state.setInitialArgsAndKwargs(initialArgsAndKwargs);

            if (pipelineProto.hasInitialState()) {
                // if we have initial state then set it
                state.setInitialState(pipelineProto.getInitialState());
            }

            // create the graph
            var graph = PipelineGraphBuilder
                    .from(state)
                    .fromProto(pipelineProto)
                    .build();

            // save graph structure to state
            graph.saveState();

            // create events
            var events = PipelineEvents.from(configuration, state);

            // create and start pipeline
            BeginPipelineHandler.from(configuration, state, graph, events).beginPipeline(context, taskRequest);

        } catch (IndexOutOfBoundsException | InvalidProtocolBufferException e) {
            var ex = new InvalidMessageTypeException("Expected a TaskRequest containing a Pipeline", e);
            state.setStatus(TaskStatus.Status.FAILED);
            respond(context, taskRequest, MessageTypes.toTaskException(taskRequest, ex));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            state.setStatus(TaskStatus.Status.FAILED);
            respond(context, taskRequest, MessageTypes.toTaskException(taskRequest, e));
        }
    }
}
