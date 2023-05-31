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
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.ArrayOfAny;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.graph.Entry;
import com.sbbsystems.statefun.tasks.graph.Group;
import com.sbbsystems.statefun.tasks.graph.PipelineGraph;
import com.sbbsystems.statefun.tasks.graph.Task;
import com.sbbsystems.statefun.tasks.groupaggregation.GroupResultAggregator;
import com.sbbsystems.statefun.tasks.serialization.TaskEntrySerializer;
import com.sbbsystems.statefun.tasks.serialization.TaskRequestSerializer;
import com.sbbsystems.statefun.tasks.serialization.TaskResultOrExceptionSerializer;
import com.sbbsystems.statefun.tasks.serialization.TaskResultSerializer;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.Id;
import org.apache.flink.statefun.sdk.Context;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.sbbsystems.statefun.tasks.util.MoreObjects.equalsAndNotNull;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;


public final class PipelineHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineHandler.class);

    private final PipelineConfiguration configuration;
    private final PipelineFunctionState state;
    private final PipelineGraph graph;
    private final GroupResultAggregator groupResultAggregator;

    public static PipelineHandler from(@NotNull PipelineConfiguration configuration,
                                       @NotNull PipelineFunctionState state,
                                       @NotNull PipelineGraph graph,
                                       @NotNull GroupResultAggregator groupResultAggregator) {

        return new PipelineHandler(
                requireNonNull(configuration),
                requireNonNull(state),
                requireNonNull(graph),
                requireNonNull(groupResultAggregator));
    }

    private PipelineHandler(PipelineConfiguration configuration,
                            PipelineFunctionState state,
                            PipelineGraph graph,
                            GroupResultAggregator groupResultAggregator) {

        this.configuration = configuration;
        this.state = state;
        this.graph = graph;
        this.groupResultAggregator = groupResultAggregator;
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
        var taskArgsAndKwargs = state.getInitialArgsAndKwargs();

        // we may have no initial tasks in the case of empty groups so continue to iterate over these
        while (initialTasks.isEmpty() && !isNull(entry)) {
            entry = graph.getNextEntry(entry);
            initialTasks = graph.getInitialTasks(entry).collect(Collectors.toUnmodifiableList());
        }

        if (initialTasks.isEmpty()) {
            // if we have a completely empty pipeline after iterating over empty groups then return empty result
            state.setStatus(TaskStatus.Status.COMPLETED);
            var taskResult = MessageTypes.toTaskResult(incomingTaskRequest, ArrayOfAny.getDefaultInstance());
            respond(context, incomingTaskRequest, taskResult);
        }
        else {
            LOG.info("We have some tasks to call at the start of the pipeline - {}", initialTasks);
            // else call the initial tasks
            for (var task: initialTasks) {

                var taskEntry = graph.getTaskEntry(task.getId());
                var outgoingTaskRequest = taskRequest.createOutgoingTaskRequest(state, taskEntry);

                // add task arguments - if previous task was empty (by definition) group then send []
                // todo we could modify this to sent nested [[]] but for now empty is consistent with python API
                var mergedArgsAndKwargs = task.getPrevious() instanceof Group
                        ? TaskEntrySerializer.of(taskEntry).mergeWith(MessageTypes.argsOfEmptyArray())
                        : TaskEntrySerializer.of(taskEntry).mergeWith(taskArgsAndKwargs);

                outgoingTaskRequest.setRequest(mergedArgsAndKwargs);

                // add initial state if present otherwise we start each pipeline with empty state
                if (!isNull(state.getInitialState())) {
                    outgoingTaskRequest.setState(state.getInitialState());
                }

                // set the reply address to be the callback function
                outgoingTaskRequest.setReplyAddress(MessageTypes.getCallbackFunctionAddress(configuration, context.self().id()));

                // send message
                context.send(MessageTypes.getSdkAddress(taskEntry), MessageTypes.wrap(outgoingTaskRequest.build()));
            }

//            // todo remove once TaskResult processing is working
            var taskResult = MessageTypes.toTaskResult(incomingTaskRequest, StringValue.of("DONE"));
            context.send(MessageTypes.getEgress(configuration), MessageTypes.toEgress(taskResult, incomingTaskRequest.getReplyTopic()));
        }
    }

    public void continuePipeline(Context context, TaskResultOrException message)
        throws StatefunTasksException {

        if (!isInThisInvocation(message)) {
            // invocation ids don't match - must be delayed result of previous invocation
            return;
        }

        if (message.hasTaskResult()) {
            LOG.info("Got a task result for {}", message.getTaskResult().getUid());
            state.setCurrentTaskState(message.getTaskException().getState());
        }

        if (message.hasTaskException()) {
            LOG.info("Got a task exception for {}", message.getTaskException().getUid());
            state.setCurrentTaskState(message.getTaskException().getState());
        }

        // N.B. that context.caller() will return callback function address not the task address
        var callerId = message.hasTaskResult() ? message.getTaskResult().getUid() : message.getTaskException().getUid();
        var completedEntry = graph.getEntry(callerId);

        // mark task complete
        graph.markComplete(completedEntry);

        // is task part of a group?
        var parentGroup = completedEntry.getParentGroup();

        // if so and we have an exception then record this to aid in aggregation later
        if (!isNull(parentGroup) && message.hasTaskException()) {
            graph.markHasException(parentGroup);
        }

        // get next entry skipping over exceptionally tasks as required
        var skippedTasks = new LinkedList<Task>();
        var nextEntry = graph.getNextEntry(completedEntry);
        var skippedAnEmptyGroup = false;

        var initialTasks = graph.getInitialTasks(nextEntry).collect(Collectors.toUnmodifiableList());
        while (nextEntry != parentGroup && skipEntry(nextEntry, initialTasks, message, skippedTasks)) {

            if (initialTasks.isEmpty()) {
                // then we have an empty group between this entry and our next entry
                skippedAnEmptyGroup = true;
            }

            graph.markComplete(nextEntry);
            nextEntry = graph.getNextEntry(nextEntry);
            initialTasks = graph.getInitialTasks(nextEntry).collect(Collectors.toUnmodifiableList());
        }

        if (skippedAnEmptyGroup) {
            // we need to pass [] as the result to the next task(s)
            message = TaskResultOrExceptionSerializer.of(message).toEmptyGroupResult();
        }

        if (equalsAndNotNull(parentGroup, nextEntry)) {
            // if the next step is the parent group this task was the end of a chain
            // save the result so that we can aggregate later
            state.getIntermediateGroupResults().set(completedEntry.getChainHead().getId(), message);

            if (graph.isComplete(parentGroup.getId())) {
                LOG.info("{} is complete", parentGroup);

                var groupResult = aggregateGroupResults(parentGroup);
                LOG.info("Got a {} group result", groupResult);

                continuePipeline(context, groupResult);
                return;
            } else {
                LOG.info("{} is not complete", parentGroup);
            }
        } else {
            LOG.info("The next entry is {}", nextEntry);

            if (!isNull(nextEntry)) {
                LOG.info("We have a next entry to call");

                if (initialTasks.isEmpty()) {
                    LOG.info("We do not have some tasks to call - initial tasks was empty");
                } else {
                    LOG.info("We have some tasks to call - {}", initialTasks);

                    // todo support for finally_do
                    var taskRequest = TaskRequestSerializer.of(state.getTaskRequest());
                    var taskResult = TaskResultSerializer.of(message);

                    for (var task: initialTasks) {
                        var taskEntry = graph.getTaskEntry(task.getId());
                        var outgoingTaskRequest = taskRequest.createOutgoingTaskRequest(state, taskEntry);

                        // add task arguments
                        var mergedArgsAndKwargs = TaskEntrySerializer.of(taskEntry).mergeWith(taskResult.getResult());
                        outgoingTaskRequest.setRequest(mergedArgsAndKwargs);

                        // set the state
                        outgoingTaskRequest.setState(taskResult.getState());

                        // set the reply address to be the callback function
                        outgoingTaskRequest.setReplyAddress(MessageTypes.getCallbackFunctionAddress(configuration, context.self().id()));

                        // send message
                        context.send(MessageTypes.getSdkAddress(taskEntry), MessageTypes.wrap(outgoingTaskRequest.build()));
                    }
                }
            }
            else {
                if (lastTaskIsCompleteOrSkipped(skippedTasks, completedEntry)) {
                    LOG.info("We are at the end of the pipeline with a result or exception");
                }
//                else if (message.hasTaskException()) {
//                    //todo not needed
//                    LOG.info("We failed and need to return an error now");
//                }
            }

            LOG.info("Skipped tasks is {}", skippedTasks);
        }
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


    private boolean skipEntry(Entry entry, List<Task> initialTasks, TaskResultOrException taskResultOrException, List<Task> skippedTasks) {
        if (isNull(entry) || initialTasks.isEmpty()) {
            return true;
        }

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

    private boolean lastTaskIsCompleteOrSkipped(List<Task> skippedTasks, Entry completedEntry) {
        if (equalsAndNotNull(completedEntry, graph.getTail())) {
            return true;
        }

        return equalsAndNotNull(Iterables.getLast(skippedTasks, null), graph.getTail());
    }

    private TaskResultOrException aggregateGroupResults(Group group) {
        var groupEntry = graph.getGroupEntry(group.getId());
        var hasException = graph.hasException(groupEntry);
        var groupResults = group
                .getItems()
                .stream()
                .map(entry -> state.getIntermediateGroupResults().get(entry.getChainHead().getId()));

        return groupResultAggregator.aggregateResults(group.getId(),
                state.getInvocationId(), groupResults, hasException, groupEntry.returnExceptions);
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
