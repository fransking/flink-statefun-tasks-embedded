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
import com.sbbsystems.statefun.tasks.generated.ArrayOfAny;
import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.graph.PipelineGraphBuilder;
import com.sbbsystems.statefun.tasks.pipeline.BeginPipelineHandler;
import com.sbbsystems.statefun.tasks.serialization.TaskRequestSerializer;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.types.UnsupportedMessageTypeException;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.List;

import static com.sbbsystems.statefun.tasks.types.MessageTypes.FLATTEN_RESULTS_TASK_TYPE;
import static com.sbbsystems.statefun.tasks.types.MessageTypes.RUN_PIPELINE_TASK_TYPE;

public final class TaskRequestHandler extends MessageHandler<TaskRequest, PipelineFunctionState> {
    private static final Logger LOG = LoggerFactory.getLogger(TaskRequestHandler.class);

    private static final List<TaskStatus.Status> ALLOWED_INITIAL_STATES = List.of(
            TaskStatus.Status.PENDING,
            TaskStatus.Status.COMPLETED,
            TaskStatus.Status.FAILED,
            TaskStatus.Status.CANCELLED
    );

    public static TaskRequestHandler with(PipelineConfiguration configuration) {
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
            switch (taskRequest.getType()) {
                case RUN_PIPELINE_TASK_TYPE:
                    runPipeline(context, taskRequest, state);
                    break;

                case FLATTEN_RESULTS_TASK_TYPE:
                    flattenResults(context, taskRequest);
                    break;

                default:
                    throw new UnsupportedMessageTypeException(MessageFormat.format("Unsupported request type {}", taskRequest.getType()));
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            state.setStatus(TaskStatus.Status.FAILED);
            respond(context, taskRequest, MessageTypes.toTaskException(taskRequest, e));
        }
    }

    private void runPipeline(Context context, TaskRequest taskRequest, PipelineFunctionState state)
            throws StatefunTasksException {

        try {

            if (!ALLOWED_INITIAL_STATES.contains(state.getStatus())) {
                throw new StatefunTasksException("Pipelines must have finished before they can be re-run");
            }

            // reset pipeline state
            state.reset();

            var taskArgsAndKwargs = TaskRequestSerializer.of(taskRequest).getArgsAndKwargsSerializer();
            var pipelineProto = taskArgsAndKwargs.getArg(0).unpack(Pipeline.class);

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
                    .withDefaultNamespace(context.self().type().namespace())  // entries without namespace will default to self
                    .withDefaultWorkerName(context.self().type().name())  // entries without worker name will default to self
                    .fromProto(pipelineProto)
                    .build();

            // save graph structure to state
            graph.saveState();

            // create events
            var events = PipelineEvents.from(configuration, state);

            // create and start pipeline
            BeginPipelineHandler.from(configuration, state, graph, events).beginPipeline(context, taskRequest);

        } catch (IndexOutOfBoundsException | InvalidProtocolBufferException e) {
            throw new InvalidMessageTypeException("Expected a TaskRequest containing a Pipeline", e);
        }
    }

    private void flattenResults(Context context, TaskRequest taskRequest)
            throws StatefunTasksException {

        try {

            var taskArgsAndKwargs = TaskRequestSerializer.of(taskRequest).getArgsAndKwargsSerializer();
            var from = taskArgsAndKwargs.getArg(0).unpack(ArrayOfAny.class);
            var into = ArrayOfAny.newBuilder();

            for (var item: from.getItemsList()) {
                into.addAllItems(item.unpack(ArrayOfAny.class).getItemsList());
            }

            var taskResult = MessageTypes.toOutgoingTaskResult(taskRequest, into.build());
            respond(context, taskRequest, taskResult);

        } catch (IndexOutOfBoundsException | InvalidProtocolBufferException e) {
            throw new InvalidMessageTypeException("Expected a TaskRequest containing an ArrayOfAny", e);
        }
    }
}
