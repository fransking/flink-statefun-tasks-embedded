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

import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.PausedTask;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.graph.DeferredTaskIds;
import com.sbbsystems.statefun.tasks.graph.Group;
import com.sbbsystems.statefun.tasks.graph.Task;
import com.sbbsystems.statefun.tasks.types.DeferredTask;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;

public class TaskSubmitter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TaskSubmitter.class);

    private final PipelineFunctionState state;
    private final Context context;
    private final HashMap<String, LinkedList<String>> deferredTaskIds = new HashMap<>();
    private final HashMap<String, Integer> taskCounts = new HashMap<>();


    public static TaskSubmitter of(PipelineFunctionState state, Context context) {
        return new TaskSubmitter(state, context);
    }

    public static void submitNextDeferredTask(PipelineFunctionState state, Context context, Group parentGroup)
            throws StatefunTasksException {

        var groupDeferredTasks = state.getDeferredTaskIds().get(parentGroup.getId());
        if (groupDeferredTasks != null && groupDeferredTasks.getTaskIds().size() > 0) {
            // deferred tasks exist for this group - submit the next one from the list
            var nextTaskId = groupDeferredTasks.getTaskIds().remove();
            var nextTask = state.getDeferredTasks().get(nextTaskId);

            // remove from state
            state.getDeferredTasks().remove(nextTaskId);
            int nRemaining = groupDeferredTasks.getTaskIds().size();
            if (nRemaining == 0) {
                state.getDeferredTaskIds().remove(parentGroup.getId());
            } else {
                state.getDeferredTaskIds().set(parentGroup.getId(), groupDeferredTasks);
            }

            // submit task
            LOG.info("Submitting deferred task {} from group {} ({} remaining)", nextTaskId, parentGroup.getId(), nRemaining);
            try {
                submitTask(context, state, nextTask.getAddress(), nextTask.getMessage());
            } catch (InvalidProtocolBufferException e) {
                throw new StatefunTasksException("Invalid TypedValue stored in state", e);
            }
        }
    }

    public static void unpauseTasks(Context context, PipelineFunctionState state) {
        for (var pausedTask: state.getPausedTasks().view()) {
            try {
                var address = MessageTypes.toSdkAddress(pausedTask.getDestination());
                var message = TypedValue.parseFrom(pausedTask.getTypedValueBytes());
                context.send(address, message);
            } catch (InvalidProtocolBufferException e) {
                LOG.error("Error parsing PausedTask", e);
            }
        }

        state.getPausedTasks().clear();
    }

    private TaskSubmitter(PipelineFunctionState state, Context context) {
        this.state = state;
        this.context = context;
    }

    public void submitOrDefer(Task task, Address address, TypedValue message)
            throws StatefunTasksException {

        var shouldDefer = false;
        var parentGroup = task.getParentGroup();
        if (parentGroup != null) {
            // keep track of the number of tasks submitted per group, and defer any above the max parallelism
            var parentGroupId = parentGroup.getId();
            var groupTaskCount = taskCounts.getOrDefault(parentGroupId, 0) + 1;

            taskCounts.put(parentGroupId, groupTaskCount);

            if (parentGroup.getMaxParallelism() > 0 && groupTaskCount > parentGroup.getMaxParallelism()) {
                shouldDefer = true;
            }
        }
        if (shouldDefer) {
            deferTask(task, address, message, parentGroup);
        } else {
            submitTask(context, state, address, message);
        }
    }

    private void deferTask(Task task, Address address, TypedValue message, Group parentGroup)
            throws StatefunTasksException {

        if (parentGroup == null) {
            throw new StatefunTasksException("Deferred tasks must have a parent group");
        }
        var parentGroupId = parentGroup.getId();
        if (!deferredTaskIds.containsKey(parentGroupId)) {
            deferredTaskIds.put(parentGroupId, new LinkedList<>());
        }
        deferredTaskIds.get(parentGroupId).add(task.getId());  // written to state on close
        var deferredTask = DeferredTask.of(address.type().namespace(), address.type().name(), address.id(), message);
        state.getDeferredTasks().set(task.getId(), deferredTask);
    }

    private static void submitTask(Context context, PipelineFunctionState state, Address address, TypedValue message) {
        var status = state.getStatus();

        if (status == TaskStatus.Status.PAUSED) {
            var pausedTask = PausedTask.newBuilder()
                    .setDestination(MessageTypes.toAddress(address))
                    .setTypedValueBytes(message.toByteString())
                    .build();

            state.getPausedTasks().append(pausedTask);
        }
        else {
            context.send(address, message);
        }
    }

    private void persistState() {
        for (var deferredTaskIdEntry : deferredTaskIds.entrySet()) {
            LOG.info("Deferred {} tasks for group {}", deferredTaskIdEntry.getValue().size(), deferredTaskIdEntry.getKey());
            state.getDeferredTaskIds().set(deferredTaskIdEntry.getKey(), DeferredTaskIds.of(deferredTaskIdEntry.getValue()));
        }
    }

    @Override
    public void close() {
        // task ID lists are persisted to state once on close rather than each time an element is added
        persistState();
    }
}
