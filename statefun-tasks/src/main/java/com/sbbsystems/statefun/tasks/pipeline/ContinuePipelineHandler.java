package com.sbbsystems.statefun.tasks.pipeline;

import com.google.common.collect.Iterables;
import com.google.protobuf.Any;
import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.events.PipelineEvents;
import com.sbbsystems.statefun.tasks.generated.ArrayOfAny;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;
import com.sbbsystems.statefun.tasks.graph.Entry;
import com.sbbsystems.statefun.tasks.graph.Group;
import com.sbbsystems.statefun.tasks.graph.PipelineGraph;
import com.sbbsystems.statefun.tasks.graph.Task;
import com.sbbsystems.statefun.tasks.groupaggregation.GroupResultAggregator;
import com.sbbsystems.statefun.tasks.serialization.TaskEntrySerializer;
import com.sbbsystems.statefun.tasks.serialization.TaskRequestSerializer;
import com.sbbsystems.statefun.tasks.serialization.TaskResultSerializer;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.Context;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.sbbsystems.statefun.tasks.util.MoreObjects.equalsAndNotNull;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public final class ContinuePipelineHandler extends PipelineHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ContinuePipelineHandler.class);

    private final GroupResultAggregator groupResultAggregator = GroupResultAggregator.newInstance();


    public static ContinuePipelineHandler from(@NotNull PipelineConfiguration configuration,
                                               @NotNull PipelineFunctionState state,
                                               @NotNull PipelineGraph graph,
                                               @NotNull PipelineEvents events) {

        return new ContinuePipelineHandler(
                requireNonNull(configuration),
                requireNonNull(state),
                requireNonNull(graph),
                requireNonNull(events));
    }

    private ContinuePipelineHandler(PipelineConfiguration configuration, PipelineFunctionState state, PipelineGraph graph, PipelineEvents events) {
        super(configuration, state, graph, events);
    }

    public void continuePipeline(Context context, TaskResultOrException message)
            throws StatefunTasksException {

        var pipelineAddress = MessageTypes.asString(context.self());
        var callerId = message.hasTaskResult() ? message.getTaskResult().getUid() : message.getTaskException().getUid();

        if (notInThisInvocation(message)) {
            // invocation ids don't match - must be delayed result of previous invocation
            LOG.warn("Mismatched invocation id in message from {} for pipeline {}", callerId, pipelineAddress);
            return;
        }

        events.notifyPipelineTaskFinished(context, message);

        // N.B. that context.caller() will return callback function address not the task address
        var completedEntry = graph.getEntry(callerId);

        if (graph.isFinally(completedEntry) && message.hasTaskResult()) {
            // if this task is the finally task then set message to saved result before finally
            message = state.getResponseBeforeFinally();
        }

        if (message.hasTaskResult()) {
            LOG.info("Got a task result from {} for pipeline {}", callerId, pipelineAddress);
            state.setCurrentTaskState(message.getTaskResult().getState());
        }

        if (message.hasTaskException()) {
            LOG.info("Got a task exception from {} for pipeline {}", callerId, pipelineAddress);
            state.setCurrentTaskState(message.getTaskException().getState());
        }

        if (completedEntry.isWait() || (message.hasTaskResult() && message.getTaskResult().getIsWait())) {
            LOG.info("Pipeline pause requested");
            pause(context);
        }

        // mark task complete
        graph.markComplete(completedEntry);

        // is task part of a group?
        var parentGroup = completedEntry.getParentGroup();

        // get next entry skipping over exceptionally tasks as required
        var nextStep = PipelineStep.next(graph, completedEntry, message.hasTaskException());

        // notify of any skipped tasks
        events.notifyPipelineTasksSkipped(context, graph, nextStep.getSkippedTasks());

        if (equalsAndNotNull(parentGroup, nextStep.getEntry())) {
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
            } else {
                TaskSubmitter.submitNextDeferredTask(state, context, parentGroup);
            }
        } else {

            LOG.info("The next entry is {} for pipeline {}", nextStep.getEntry(), pipelineAddress);

            if (!isNull(nextStep.getEntry())) {
                LOG.info("Pipeline {} is continuing with {} tasks to call", pipelineAddress, nextStep.numTasksToCall());

                try (var taskSubmitter = TaskSubmitter.of(state, context)) {

                    var taskRequest = TaskRequestSerializer.of(state.getTaskRequest());
                    var taskResult = TaskResultSerializer.of(message);

                    for (var task : nextStep.getTasksToCall()) {
                        submitTask(context, taskSubmitter, task, message, taskRequest, taskResult);
                    }
                }
            }
            else {
                if (lastTaskIsCompleteOrSkipped(nextStep.getSkippedTasks(), completedEntry)) {
                    if (message.hasTaskResult()) {
                        LOG.info("Pipeline {} completed successfully", pipelineAddress);
                        respondWithResult(context, state.getTaskRequest(), message.getTaskResult());
                    } else {
                        LOG.info("Pipeline {} completed with error", pipelineAddress);
                        respondWithError(context, state.getTaskRequest(), message.getTaskException());
                    }

                } else if (message.hasTaskException()) {
                    if (isNull(parentGroup) || graph.isComplete(parentGroup.getId())) {
                        // end pipeline waiting for group to complete so we get the full aggregate exception
                        LOG.info("Pipeline {} failed", pipelineAddress);
                        respondWithError(context, state.getTaskRequest(), message.getTaskException());
                    }
                }
            }
        }
    }

    private void submitTask(Context context, TaskSubmitter taskSubmitter, Task task, TaskResultOrException message, TaskRequestSerializer taskRequest, TaskResultSerializer taskResult) throws StatefunTasksException {
        var entry = graph.getTaskEntry(task.getId());
        var taskEntry = TaskEntrySerializer.of(entry);

        var outgoingTaskRequest = taskRequest.createOutgoingTaskRequest(state, entry);
        outgoingTaskRequest
                .setReplyAddress(MessageTypes.getCallbackFunctionAddress(configuration, context.self().id()))
                .setRequest(mergeArgsAndKwargs(task, taskEntry, taskResult, message))
                .setState(taskResult.getState());

        // send message
        taskSubmitter.submitOrDefer(task, MessageTypes.getSdkAddress(entry), MessageTypes.wrap(outgoingTaskRequest.build()));
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

    private boolean lastTaskIsCompleteOrSkipped(List<Task> skippedTasks, Entry completedEntry) {
        if (equalsAndNotNull(completedEntry, graph.getTail())) {
            return true;
        }

        return equalsAndNotNull(Iterables.getLast(skippedTasks, null), graph.getTail());
    }
}
