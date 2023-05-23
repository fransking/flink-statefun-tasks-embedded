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
import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.graph.PipelineGraphBuilder;
import com.sbbsystems.statefun.tasks.pipeline.PipelineHandler;
import com.sbbsystems.statefun.tasks.serialization.TaskRequestSerializer;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TaskRequestHandler extends MessageHandler<TaskRequest, PipelineFunctionState> {
    private static final Logger LOG = LoggerFactory.getLogger(TaskRequestHandler.class);

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
                    .newInstance()
                    .withTaskEntries(state.getTaskEntries())
                    .withGroupEntries(state.getGroupEntries())
                    .withEntries(state.getEntries().getItems())
                    .fromProto(pipelineProto)
                    .build();

            // save graph structure to state
            graph.saveState(state);

            // create and start pipeline
            var pipelineHandler = PipelineHandler.from(context, state, graph);
            pipelineHandler.beginPipeline(taskRequest);

            try {
                var group = graph.getGroupEntry(graph.getHead().getId());
                group.remaining = 0;
                graph.updateGroupEntry(group);
                var group2 = graph.getGroupEntry(graph.getHead().getId());
                LOG.info(String.valueOf(group == group2));
                LOG.info(String.valueOf(group2.remaining));
            } catch (Exception e) {}

//            if (!Objects.isNull(graph.getHead())) {
//
//                var initialTasks = graph.getInitialTasks();
//
//                for (var task : initialTasks.getTasks()) {
//                    var taskEntry = graph.getTaskEntry(task.getId());
//                    LOG.info(taskEntry.taskType);
//                }
//
//                LOG.info(String.valueOf(state.getTasks().getItems().size()));
//                LOG.info(pipelineProto.toString());
//            }
        }
        catch (InvalidProtocolBufferException e) {
            throw new InvalidMessageTypeException("Expected a TaskRequest containing a Pipeline", e);
        }


//        var request = taskRequest.getRequest();
//        LOG.info(request.toString());

//        if (Iterables.isEmpty(state.getTaskEntries().entries())) {
//            for (int i = 0; i < 1000000; i++) {
//                var id = String.valueOf(UUID.randomUUID());
//                state.getTaskEntries().set(id, new TaskEntry());
//            }
//        }
//        else {
//            var id = String.valueOf(UUID.randomUUID());
//            state.getTaskEntries().set(id, new TaskEntry());
//        }
//
//        LOG.info(String.valueOf(Iterables.size(state.getTaskEntries().keys())));

        LOG.info("GOT A TASK_REQUEST");

        LOG.info("TASK_REQUEST_ID is {}", taskRequest.getId());

        var egress = new EgressIdentifier<>(configuration.getEgressNamespace(), configuration.getEgressType(), TypedValue.class);
        var taskResult = TaskResult.newBuilder()
                .setId(taskRequest.getId())
                .setUid(taskRequest.getUid())
                .build();

        context.send(egress, MessageTypes.toEgress(taskResult, taskRequest.getReplyTopic()));
    }
}
