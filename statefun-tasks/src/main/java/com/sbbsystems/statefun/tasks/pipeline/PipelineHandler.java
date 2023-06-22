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
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.events.PipelineEvents;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.graph.Entry;
import com.sbbsystems.statefun.tasks.graph.Group;
import com.sbbsystems.statefun.tasks.graph.PipelineGraph;
import com.sbbsystems.statefun.tasks.graph.Task;
import com.sbbsystems.statefun.tasks.groupaggregation.GroupResultAggregator;
import com.sbbsystems.statefun.tasks.serialization.TaskEntrySerializer;
import com.sbbsystems.statefun.tasks.serialization.TaskRequestSerializer;
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

    private final GroupResultAggregator groupResultAggregator = GroupResultAggregator.newInstance();
    private final PipelineConfiguration configuration;
    private final PipelineFunctionState state;
    private final PipelineGraph graph;
    private final PipelineEvents events;

    public static PipelineHandler from(@NotNull PipelineConfiguration configuration,
                                       @NotNull PipelineFunctionState state,
                                       @NotNull PipelineGraph graph,
                                       @NotNull PipelineEvents events) {

        return new PipelineHandler(
                requireNonNull(configuration),
                requireNonNull(state),
                requireNonNull(graph),
                requireNonNull(events));
    }

    private PipelineHandler(PipelineConfiguration configuration,
                            PipelineFunctionState state,
                            PipelineGraph graph,
                            PipelineEvents events) {

        this.configuration = configuration;
        this.state = state;
        this.graph = graph;
        this.events = events;
    }

    public void beginPipeline(Context context, TaskRequest incomingTaskRequest)
            throws StatefunTasksException {

        var taskRequest = TaskRequestSerializer.of(incomingTaskRequest);
        var pipelineAddress = MessageTypes.asString(context.self());

        state.setPipelineAddress(MessageTypes.toAddress(context.self()));
        state.setRootPipelineAddress(taskRequest.getRootPipelineAddress(context));
        state.setTaskRequest(incomingTaskRequest);
        state.setInvocationId(Id.generate());
        state.setStatus(TaskStatus.Status.RUNNING);

        if (incomingTaskRequest.hasField(TaskRequest.getDescriptor().findFieldByNumber(TaskRequest.IS_FRUITFUL_FIELD_NUMBER))) {
            state.setIsFruitful(incomingTaskRequest.getIsFruitful());
        }

        if (!isNull(context.caller())) {
            state.setCallerAddress(MessageTypes.toAddress(context.caller()));
        }

        if (state.getIsInline() && isNull(state.getInitialState())) {
            // if this is an inline pipeline and we haven't been given pipeline initial state
            // then copy incomingTaskRequest state to pipeline current state
            state.setInitialState(incomingTaskRequest.getState());
        }

        // set latest state of pipeline equal to initial state
        state.setCurrentTaskState(state.getInitialState());

        // notify pipeline created (events + root pipeline)
        events.notifyPipelineCreated(context);

        var entry = graph.getHead();

        if (isNull(entry)) {
            throw new StatefunTasksException("Cannot run an empty pipeline");
        }

        // get the initial tasks to call and the args and kwargs
        var skippedTasks = new LinkedList<Task>();
        var initialTasks = getInitialTasks(entry, false, skippedTasks);

        // we may have no initial tasks in the case of empty groups so continue to iterate over these
        while (initialTasks.isEmpty() && !isNull(entry)) {
            graph.markComplete(entry);
            entry = graph.getNextEntry(entry);
            initialTasks = getInitialTasks(entry, false, skippedTasks);
        }

        if (initialTasks.isEmpty()) {
            // if we have a completely empty pipeline after iterating over empty groups then return empty result
            LOG.info("Pipeline {} is empty, returning []", pipelineAddress);
            var taskResult = MessageTypes.toOutgoingTaskResult(incomingTaskRequest, ArrayOfAny.getDefaultInstance());
            respondWithResult(context, incomingTaskRequest, taskResult);
        }
        else {
            LOG.info("Pipeline {} is starting with {} tasks to call", pipelineAddress, initialTasks.size());
            events.notifyPipelineStatusChanged(context, TaskStatus.Status.RUNNING);

            var argsAndKwargs = state.getInitialArgsAndKwargs();

            // else call the initial tasks
            for (var task: initialTasks) {

                var taskEntry = graph.getTaskEntry(task.getId());
                var outgoingTaskRequest = taskRequest.createOutgoingTaskRequest(state, taskEntry);

                // add task arguments - if we skipped any empty groups then we have to send [] to the next task
                var mergedArgsAndKwargs = task.isPrecededByAnEmptyGroup()
                        ? TaskEntrySerializer.of(taskEntry).mergeWith(MessageTypes.argsOfEmptyArray())
                        : TaskEntrySerializer.of(taskEntry).mergeWith(argsAndKwargs);

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
            LOG.info("Pipeline {} started", pipelineAddress);
        }
    }

    public void continuePipeline(Context context, TaskResultOrException message)
        throws StatefunTasksException {

        var pipelineAddress = MessageTypes.asString(context.self());
        var callerId = message.hasTaskResult() ? message.getTaskResult().getUid() : message.getTaskException().getUid();

        if (!isInThisInvocation(message)) {
            // invocation ids don't match - must be delayed result of previous invocation
            LOG.warn("Mismatched invocation id in message from {} for pipeline {}", callerId, pipelineAddress);
            return;
        }

        // N.B. that context.caller() will return callback function address not the task address
        var completedEntry = graph.getEntry(callerId);

        if (graph.isFinally(completedEntry) && message.hasTaskResult()) {
            // if this task is the finally task then set message to saved result before finally
            message = state.getResponseBeforeFinally();
        }

        if (message.hasTaskResult()) {
            LOG.info("Got a task result from {} for pipeline {}", callerId, pipelineAddress);
            state.setCurrentTaskState(message.getTaskException().getState());
        }

        if (message.hasTaskException()) {
            LOG.info("Got a task exception from {} for pipeline {}", callerId, pipelineAddress);
            state.setCurrentTaskState(message.getTaskException().getState());
        }

        // mark task complete
        graph.markComplete(completedEntry);

        // is task part of a group?
        var parentGroup = completedEntry.getParentGroup();

        // get next entry skipping over exceptionally tasks as required
        var skippedTasks = new LinkedList<Task>();
        var nextEntry = graph.getNextEntry(completedEntry);

        var initialTasks = nextEntry == parentGroup
                ? List.<Task>of()
                : getInitialTasks(nextEntry, message.hasTaskException(), skippedTasks);

        while (nextEntry != parentGroup && initialTasks.isEmpty()) {
            graph.markComplete(nextEntry);
            nextEntry = graph.getNextEntry(nextEntry);
            initialTasks = getInitialTasks(nextEntry, message.hasTaskException(), skippedTasks);
        }

        if (equalsAndNotNull(parentGroup, nextEntry)) {
            // if the next step is the parent group this task was the end of a chain
            // save the result so that we can aggregate later
            LOG.info("Chain {} in {} is complete", completedEntry.getChainHead(), parentGroup);
            state.getIntermediateGroupResults().set(completedEntry.getChainHead().getId(), message);

            if (message.hasTaskException()) {
                // if we have an exception then record this to aid in aggregation later
                graph.markHasException(parentGroup);
            }

            if (graph.isComplete(parentGroup.getId())) {
                LOG.info("{} is complete", parentGroup);
                continuePipeline(context, aggregateGroupResults(parentGroup));
            }
        } else {

            LOG.info("The next entry is {} for pipeline {}", nextEntry, pipelineAddress);

            if (!isNull(nextEntry)) {
                LOG.info("Pipeline {} is continuing with {} tasks to call", pipelineAddress, initialTasks.size());

                var taskRequest = TaskRequestSerializer.of(state.getTaskRequest());
                var taskResult = TaskResultSerializer.of(message);

                for (var task: initialTasks) {
                    var entry = graph.getTaskEntry(task.getId());
                    var taskEntry = TaskEntrySerializer.of(entry);

                    var outgoingTaskRequest = taskRequest.createOutgoingTaskRequest(state, entry);

                    // add task arguments
                    var mergedArgsAndKwargs = mergeArgsAndKwargs(task, taskEntry, taskResult, message);

                    outgoingTaskRequest.setRequest(mergedArgsAndKwargs);

                    // set the state
                    outgoingTaskRequest.setState(taskResult.getState());

                    // set the reply address to be the callback function
                    outgoingTaskRequest.setReplyAddress(MessageTypes.getCallbackFunctionAddress(configuration, context.self().id()));

                    // send message
                    context.send(MessageTypes.getSdkAddress(entry), MessageTypes.wrap(outgoingTaskRequest.build()));
                }
            }
            else {
                if (lastTaskIsCompleteOrSkipped(skippedTasks, completedEntry)) {
                    if (message.hasTaskResult()) {
                        LOG.info("Pipeline {} completed successfully", pipelineAddress);
                        respondWithResult(context, state.getTaskRequest(), message.getTaskResult());
                    } else {
                        LOG.info("Pipeline {} completed with error", pipelineAddress);
                        respondWithError(context, state.getTaskRequest(), message.getTaskException());
                    }

                } else if (message.hasTaskResult()) {
                    if (isNull(parentGroup) || graph.isComplete(parentGroup.getId())) {
                        // end pipeline waiting for group to complete so we get the full aggregate exception
                        LOG.info("Pipeline {} failed", pipelineAddress);
                        respondWithError(context, state.getTaskRequest(), message.getTaskException());
                    }
                }
            }
        }
    }

    private Any mergeArgsAndKwargs(Task task, TaskEntrySerializer taskEntry, TaskResultSerializer taskResult, TaskResultOrException message)
            throws StatefunTasksException {

        if (task.isFinally()) {
            state.setResponseBeforeFinally(message);
            return taskEntry.mergeWith(TupleOfAny.getDefaultInstance());
        } else if (task.isPrecededByAnEmptyGroup() && !message.hasTaskException()) {
            return taskEntry.mergeWith(ArrayOfAny.getDefaultInstance());
        } else {
            return taskEntry.mergeWith(taskResult.getResult());
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

    private List<Task> getInitialTasks(Entry entry, boolean exceptionally, List<Task> skippedTasks) {
        return graph.getInitialTasks(entry, exceptionally, skippedTasks).collect(Collectors.toUnmodifiableList());
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

    private void respondWithResult(@NotNull Context context, TaskRequest taskRequest, TaskResult taskResult) {
        var result = state.getIsFruitful()
                ? taskResult.getResult()
                : MessageTypes.tupleOfEmptyArray();

        var outgoingTaskResult = state.getIsInline()
                ? MessageTypes.toOutgoingTaskResult(taskRequest, result, taskResult.getState())
                : MessageTypes.toOutgoingTaskResult(taskRequest, result);

        state.setTaskResult(outgoingTaskResult);
        state.setStatus(TaskStatus.Status.COMPLETED);
        events.notifyPipelineStatusChanged(context, TaskStatus.Status.COMPLETED);
        respond(context, taskRequest, outgoingTaskResult);
    }

    private void respondWithError(@NotNull Context context, TaskRequest taskRequest, TaskException error) {
        var outgoingTaskException = state.getIsInline()
                ? MessageTypes.toOutgoingTaskException(taskRequest, error, error.getState())
                : MessageTypes.toOutgoingTaskException(taskRequest, error);

        state.setTaskException(outgoingTaskException);
        state.setStatus(TaskStatus.Status.FAILED);
        events.notifyPipelineStatusChanged(context, TaskStatus.Status.FAILED);
        respond(context, taskRequest, outgoingTaskException);
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
