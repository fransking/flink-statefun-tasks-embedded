package com.sbbsystems.statefun.tasks.pipeline;

import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.events.PipelineEvents;
import com.sbbsystems.statefun.tasks.generated.ArgsAndKwargs;
import com.sbbsystems.statefun.tasks.generated.ArrayOfAny;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
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

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public final class BeginPipelineHandler extends PipelineHandler {
    private static final Logger LOG = LoggerFactory.getLogger(BeginPipelineHandler.class);

    public static BeginPipelineHandler from(@NotNull PipelineConfiguration configuration,
                                       @NotNull PipelineFunctionState state,
                                       @NotNull PipelineGraph graph,
                                       @NotNull PipelineEvents events) {

        return new BeginPipelineHandler(
                requireNonNull(configuration),
                requireNonNull(state),
                requireNonNull(graph),
                requireNonNull(events));
    }

    private BeginPipelineHandler(PipelineConfiguration configuration, PipelineFunctionState state, PipelineGraph graph, PipelineEvents events) {
        super(configuration, state, graph, events);
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
        events.notifyPipelineCreated(context, graph);

        // get the initial tasks to call
        var initialStep = PipelineStep.fromHead(graph);

        // notify of any skipped tasks
        events.notifyPipelineTasksSkipped(context, graph, initialStep.getSkippedTasks());

        if (initialStep.hasNoTasksToCall()) {
            // if we have a completely empty pipeline after iterating over empty groups then return empty result
            LOG.info("Pipeline {} is empty, returning []", pipelineAddress);
            var taskResult = MessageTypes.toOutgoingTaskResult(incomingTaskRequest, ArrayOfAny.getDefaultInstance());
            respondWithResult(context, incomingTaskRequest, taskResult);
        }
        else {
            LOG.debug("Pipeline {} is starting with {} tasks to call", pipelineAddress, initialStep.numTasksToCall());
            events.notifyPipelineStatusChanged(context, TaskStatus.Status.RUNNING);

            try (var taskSubmitter = TaskSubmitter.of(state, context)) {
                var argsAndKwargs = state.getInitialArgsAndKwargs();

                // else call the initial tasks
                for (var task : initialStep.getTasksToCall()) {
                    submitTask(context, taskSubmitter, task, taskRequest, argsAndKwargs);
                }
            }
            LOG.info("Pipeline {} started", pipelineAddress);
        }
    }

    private void submitTask(Context context, TaskSubmitter taskSubmitter, Task task, TaskRequestSerializer taskRequest, ArgsAndKwargs argsAndKwargs)
            throws StatefunTasksException {

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
        taskSubmitter.submitOrDefer(task, MessageTypes.getSdkAddress(taskEntry), MessageTypes.wrap(outgoingTaskRequest.build()));
    }
}
