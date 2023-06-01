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
package com.sbbsystems.statefun.tasks.serialization;

import com.google.protobuf.Any;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.Address;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.types.TaskEntry;
import org.apache.flink.statefun.sdk.Context;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public final class TaskRequestSerializer {
    private final TaskRequest taskRequest;

    public static TaskRequestSerializer of(TaskRequest taskRequest) {
        return new TaskRequestSerializer(taskRequest);
    }

    private TaskRequestSerializer(TaskRequest taskRequest) {
        this.taskRequest = taskRequest;
    }

    public ArgsAndKwargsSerializer getArgsAndKwargsSerializer()
            throws StatefunTasksException {

        return ArgsAndKwargsSerializer.of(taskRequest.getRequest());
    }

    public Address getRootPipelineAddress(Context context) {
        var rootPipelineAddress =  taskRequest.getMetaOrDefault("root_pipeline_address", MessageTypes.toTypeName(context.self()));
        var rootPipelineId =  taskRequest.getMetaOrDefault("root_pipeline_id", context.self().id());
        return MessageTypes.toAddress(rootPipelineAddress, rootPipelineId);
    }

    public TaskRequest.Builder createOutgoingTaskRequest(PipelineFunctionState state, TaskEntry taskEntry) {

        var outgoingTaskRequest = TaskRequest.newBuilder()
                .setId(taskEntry.taskId)
                .setUid(taskEntry.uid)
                .setType(taskEntry.taskType)
                .setInvocationId(state.getInvocationId());

        // set meta data properties
        var pipelineAddress = MessageTypes.toTypeName(state.getPipelineAddress());
        var pipelineId = state.getPipelineAddress().getId();
        outgoingTaskRequest.putMeta("pipeline_address", pipelineAddress);
        outgoingTaskRequest.putMeta("pipeline_id", pipelineId);

        var rootPipelineAddress =  taskRequest.getMetaOrDefault("root_pipeline_address", pipelineAddress);
        var rootPipelineId =  taskRequest.getMetaOrDefault("root_pipeline_id", pipelineId);
        outgoingTaskRequest.putMeta("root_pipeline_address", rootPipelineAddress);
        outgoingTaskRequest.putMeta("root_pipeline_id", rootPipelineId);

        if (!Objects.isNull(state.getCallerAddress())) {
            outgoingTaskRequest.putMeta("parent_task_address", MessageTypes.toTypeName(state.getCallerAddress()));
            outgoingTaskRequest.putMeta("parent_task_id", state.getCallerAddress().getId());
        }

        if (state.getIsInline()) {
            var inlineParentPipelineAddress = taskRequest.getMetaOrDefault("inline_parent_pipeline_address", pipelineAddress);
            var inlineParentPipelineId = taskRequest.getMetaOrDefault("inline_parent_pipeline_id", pipelineId);
            outgoingTaskRequest.putMeta("inline_parent_pipeline_address", inlineParentPipelineAddress);
            outgoingTaskRequest.putMeta("inline_parent_pipeline_id", inlineParentPipelineId);
        }

        outgoingTaskRequest.putMeta("display_name", taskEntry.displayName);

        return outgoingTaskRequest;
    }
}
