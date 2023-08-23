package com.sbbsystems.statefun.tasks.pipeline;

import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.events.PipelineEvents;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.graph.PipelineGraph;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.Context;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public final class CancelPipelineHandler extends PipelineHandler {
    private static final Logger LOG = LoggerFactory.getLogger(CancelPipelineHandler.class);

    public static CancelPipelineHandler from(@NotNull PipelineConfiguration configuration,
                                             @NotNull PipelineFunctionState state,
                                             @NotNull PipelineGraph graph,
                                             @NotNull PipelineEvents events) {

        return new CancelPipelineHandler(
                requireNonNull(configuration),
                requireNonNull(state),
                requireNonNull(graph),
                requireNonNull(events));
    }

    private CancelPipelineHandler(PipelineConfiguration configuration, PipelineFunctionState state, PipelineGraph graph, PipelineEvents events) {
        super(configuration, state, graph, events);
    }

    public void cancelPipeline(Context context, TaskResultOrException message) {

        var pipelineAddress = MessageTypes.asString(context.self());
        var callerId = message.hasTaskResult() ? message.getTaskResult().getUid() : message.getTaskException().getUid();

        if (notInThisInvocation(message)) {
            // invocation ids don't match - must be delayed result of previous invocation
            LOG.warn("Mismatched invocation id in message from {} for pipeline {}", callerId, pipelineAddress);
            return;
        }

        var completedEntry = graph.getEntry(callerId);

        if (isNull(completedEntry)) {
            // check if this is the pipeline calling back to itself to cancel
            if (!state.getPipelineAddress().getId().equals(callerId) || !message.hasTaskException()) {
                return;
            }

        } else {
            // or wait for the finally
            graph.markComplete(completedEntry);

            if (!graph.isFinally(completedEntry)) {
                return;
            }

            message = state.getResponseBeforeFinally();  // overwritten by PipelineHandler.cancel()
        }

        var taskRequest = state.getTaskRequest();
        var taskException = message.getTaskException();
        var outgoingTaskException = state.getIsInline()
                ? MessageTypes.toOutgoingTaskException(taskRequest, taskException, taskException.getState())
                : MessageTypes.toOutgoingTaskException(taskRequest, taskException);

        state.setTaskException(outgoingTaskException);
        state.setStatus(TaskStatus.Status.CANCELLED);
        events.notifyPipelineStatusChanged(context, TaskStatus.Status.CANCELLED);

        respond(context, taskRequest, outgoingTaskException);
    }
}
