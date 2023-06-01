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

import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.graph.Entry;
import com.sbbsystems.statefun.tasks.graph.Group;
import com.sbbsystems.statefun.tasks.graph.PipelineGraph;
import com.sbbsystems.statefun.tasks.graph.Task;
import com.sbbsystems.statefun.tasks.serialization.TaskEntrySerializer;
import com.sbbsystems.statefun.tasks.serialization.TaskRequestSerializer;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.Id;
import org.apache.flink.statefun.sdk.Context;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static com.sbbsystems.statefun.tasks.util.MoreObjects.equalsAndNotNull;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.isNull;


public final class PipelineHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineHandler.class);

    private final PipelineConfiguration configuration;
    private final PipelineFunctionState state;
    private final PipelineGraph graph;

    public static PipelineHandler from(@NotNull PipelineConfiguration configuration,
                                       @NotNull PipelineFunctionState state,
                                       @NotNull PipelineGraph graph) {
        return new PipelineHandler(
                requireNonNull(configuration),
                requireNonNull(state),
                requireNonNull(graph));
    }

    private PipelineHandler(PipelineConfiguration configuration, PipelineFunctionState state, PipelineGraph graph) {
        this.configuration = configuration;
        this.state = state;
        this.graph = graph;
    }

    public void beginPipeline(Context context, TaskRequest incomingTaskRequest)
        throws StatefunTasksException {

        var taskRequest = TaskRequestSerializer.of(incomingTaskRequest);

        state.setPipelineAddress(MessageTypes.toAddress(context.self()));
        state.setRootPipelineAddress(taskRequest.getRootPipelineAddress(context));
        state.setTaskRequest(incomingTaskRequest);
        state.setInvocationId(Id.generate());
        state.setIsFruitful(incomingTaskRequest.getIsFruitful()); // todo move this into the calling task
        state.setStatus(TaskStatus.Status.RUNNING);

        if (!isNull(context.caller())) {
            state.setCallerAddress(MessageTypes.toAddress(context.caller()));
        }

        if (state.getIsInline()) {
            // if this is an inline pipeline then copy incomingTaskRequest state to pipeline initial state
            state.setInitialState(incomingTaskRequest.getState());
        }

        // set latest state of pipeline equal to initial state
        state.setCurrentTaskState(state.getInitialState());

        var entry = graph.getHead();

        if (isNull(entry)) {
            throw new StatefunTasksException("Cannot run an empty pipeline");
        }

        // get the initial tasks to call and the args and kwargs
        var initialTasks = graph.getInitialTasks(entry).collect(Collectors.toUnmodifiableList());
        var initialArgsAndKwargs = state.getInitialArgsAndKwargs();

        // we may have no initial tasks in the case of empty groups so continue to iterate over these
        // note that implicitly, the result of an empty group is [] so we update initialArgsAndKwargs accordingly
        while (initialTasks.isEmpty() && !isNull(entry)) {
            initialArgsAndKwargs = MessageTypes.argsOfEmptyArray();

            entry = graph.getNextEntry(entry);
            initialTasks = graph.getInitialTasks(entry).collect(Collectors.toUnmodifiableList());
        }

        if (initialTasks.isEmpty()) {
            // if we have a completely empty pipeline after iterating over empty groups then return empty result
            state.setStatus(TaskStatus.Status.COMPLETED);
            var taskResult = MessageTypes.toTaskResult(incomingTaskRequest, ArrayOfAny.getDefaultInstance());
            context.send(MessageTypes.getEgress(configuration), MessageTypes.toEgress(taskResult, incomingTaskRequest.getReplyTopic()));
        }
        else {
            // else call the initial tasks
            for (var task: initialTasks) {
                var taskEntry = graph.getTaskEntry(task.getId());
                var outgoingTaskRequest = taskRequest.createOutgoingTaskRequest(state, taskEntry);

                // add task arguments
                var taskArgsAndKwargs = TaskEntrySerializer.of(taskEntry).mergeWith(initialArgsAndKwargs);
                outgoingTaskRequest.setRequest(taskArgsAndKwargs);

                // add initial state if present otherwise we start each pipeline with empty state
                if (!isNull(state.getInitialState())) {
                    outgoingTaskRequest.setState(state.getInitialState());
                }

                // set the reply address to be the callback function
                outgoingTaskRequest.setReplyAddress(MessageTypes.getCallbackFunctionAddress(configuration, context.self().id()));

                // send message
                context.send(MessageTypes.getSdkAddress(taskEntry), MessageTypes.wrap(outgoingTaskRequest.build()));
            }

            // todo remove once TaskResult processing is working
            var taskResult = MessageTypes.toTaskResult(incomingTaskRequest, StringValue.of("DONE"));
            context.send(MessageTypes.getEgress(configuration), MessageTypes.toEgress(taskResult, incomingTaskRequest.getReplyTopic()));
        }
    }

    public void continuePipeline(Context context, TaskResultOrException message) {

        if (!isInThisInvocation(message)) {
            // invocation ids don't match - must be delayed result of previous invocation
            return;
        }

        if (message.hasTaskResult()) {
            LOG.info("Got a task result for {}", message.getTaskResult().getUid());
        }

        if (message.hasTaskException()) {
            LOG.info("Got a task exception for {}", message.getTaskException().getUid());
        }

        // N.B. that context.caller() will return callback function address not the task address
        var callerId = message.hasTaskResult() ? message.getTaskResult().getUid() : message.getTaskException().getUid();
        var completedTask = graph.getEntry(callerId);

        // mark task complete
        graph.markComplete(completedTask);

        // is task part of a group?
        var parentGroup = completedTask.getParentGroup();

        // get next entry skipping over exceptionally tasks as required
        var skippedTasks = new LinkedList<Task>();
        var nextEntry = graph.getNextEntry(completedTask);

        while (nextEntry != parentGroup && skipEntry(nextEntry, message, skippedTasks)) {
            graph.markComplete(nextEntry);
            nextEntry = graph.getNextEntry(nextEntry);
        }

        if (equalsAndNotNull(parentGroup, nextEntry)) {
            // if the next step is the parent group this task was the end of a chain and the group may now be complete
            // todo record the result against the group

            if (graph.isComplete(parentGroup.getId())) {
                LOG.info("Group is complete");

//                // todo get the entry after this group
//                var entryAfterGroup = graph.getNextEntry(nextEntry);
//
//                // if it is null go up a level if we can
//                parentGroup = nextEntry.getParentGroup();
//                entryAfterGroup = graph.getNextEntry(parentGroup);
//
//                // repeat until no parent group left
//
//                // nextEntry is entryAfterGroup
//                // and if not null then get value of this group aggregated down
//                // either it is task result or task exception
//                // call back in to continuePipeline with result of this group

                // todo ignore above only aggregate when we have a next entry or
                // todo we hit the tail and it is time to egress result

                var toe = TaskResultOrException.newBuilder().setTaskResult(
                        TaskResult.newBuilder()
                                .setId(parentGroup.getId())
                                .setUid(parentGroup.getId())
                                .setInvocationId(state.getInvocationId())
                );

                continuePipeline(context, toe.build());
                return;
                // end todo
            } else {
                LOG.info("Group is not complete");
            }
        }

        LOG.info("The next entry is {}", nextEntry);
    }

    private boolean isInThisInvocation(TaskResultOrException message) {
        if (message.hasTaskResult()) {
            return message.getTaskResult().getInvocationId().equals(state.getInvocationId());
        }

        if (message.hasTaskException()) {
            return message.getTaskException().getInvocationId().equals(state.getInvocationId());
        }

        return false;
    }

    private boolean skipEntry(Entry entry, TaskResultOrException taskResultOrException, List<Task> skippedTasks) {
        if (entry instanceof Group && taskResultOrException.hasTaskException()) {
            var group = (Group) entry;
            for (var groupEntry: group.getItems()) {
                graph.getTasks(groupEntry).forEach(skippedTasks::add);
            }
            return true;
        }

        if (entry instanceof Task) {
            var task = (Task) entry;

            if (!task.isFinally() && task.isExceptionally() != taskResultOrException.hasTaskException()) {
                skippedTasks.add(task);
                return true;
            }
        }

        return false;
    }
}
