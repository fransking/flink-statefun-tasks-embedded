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
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.ArgsAndKwargs;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.graph.PipelineGraph;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.Context;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;


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

    public void beginPipeline(Context context, TaskRequest taskRequest) {
        try {
            state.setStatus(TaskStatus.Status.RUNNING);

            var initialTasks = graph.getInitialTasks();

            if (Iterables.isEmpty(initialTasks)) {
                throw new StatefunTasksException("Cannot run an empty pipeline");
            }

            var taskResult = TaskResult.newBuilder()
                    .setId(taskRequest.getId())
                    .setUid(taskRequest.getUid())

                    .build();

            context.send(MessageTypes.getEgress(configuration), MessageTypes.toEgress(taskResult, taskRequest.getReplyTopic()));
        }
        catch (StatefunTasksException e) {
            state.setStatus(TaskStatus.Status.FAILED);
            respond(context, taskRequest, MessageTypes.toTaskException(taskRequest, e));
        }
    }

    private void respond(@NotNull Context context, TaskRequest taskRequest, Message message) {
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
