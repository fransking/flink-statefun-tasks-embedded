package com.sbbsystems.statefun.tasks.events;

import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.generated.ChildPipeline;
import com.sbbsystems.statefun.tasks.generated.PipelineCreated;
import com.sbbsystems.statefun.tasks.generated.PipelineStatusChanged;
import com.sbbsystems.statefun.tasks.generated.TaskStatus;
import com.sbbsystems.statefun.tasks.graph.PipelineGraph;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.TimedBlock;
import org.apache.flink.statefun.sdk.Context;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

import static com.sbbsystems.statefun.tasks.util.MoreObjects.notEqualsAndNotNull;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class PipelineEvents {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineEvents.class);

    private final PipelineConfiguration configuration;
    private final PipelineFunctionState state;

    public static PipelineEvents from(@NotNull PipelineConfiguration configuration,
                                      @NotNull PipelineFunctionState state) {

        return new PipelineEvents(
                requireNonNull(configuration),
                requireNonNull(state));
    }

    private PipelineEvents(PipelineConfiguration configuration,
                           PipelineFunctionState state) {
        this.configuration = configuration;
        this.state = state;
    }
    
    public void notifyPipelineCreated(Context context, PipelineGraph graph) {
        var pipelineAddress = state.getPipelineAddress();
        var rootPipelineAddress = state.getRootPipelineAddress();
        var callerAddress = state.getCallerAddress();

        try (var ignored = TimedBlock.of(LOG::info, "Notifying new pipeline {0} created", MessageTypes.asString(pipelineAddress))) {
            var tasks = graph.getTaskEntries().map(MessageTypes::toTaskInfo).collect(Collectors.toUnmodifiableList());

            if (configuration.hasEventsEgress()) {
                // if we have events egress then publish PipelineCreated message
                var pipelineCreated = PipelineCreated.newBuilder().addAllTasks(tasks);

                if (!isNull(callerAddress)) {
                    pipelineCreated.setCallerId(callerAddress.getId()).setCallerAddress(MessageTypes.toTypeName(callerAddress));
                }

                var event = MessageTypes.buildEventFor(state).setPipelineCreated(pipelineCreated);
                context.send(MessageTypes.getEventsEgress(configuration), MessageTypes.toEgress(event.build(), configuration.getEventsTopic()));
            }

            if (notEqualsAndNotNull(pipelineAddress, rootPipelineAddress)) {
                // if this is a child pipeline then notify the root pipeline of a new descendant
                var childPipeline = ChildPipeline.newBuilder()
                        .setInvocationId(state.getInvocationId())
                        .setId(pipelineAddress.getId())
                        .setAddress(MessageTypes.toTypeName(pipelineAddress))
                        .setRootId(rootPipelineAddress.getId())
                        .setRootAddress(MessageTypes.toTypeName(rootPipelineAddress))
                        .addAllTasks(tasks);

                if (!isNull(callerAddress)) {
                    childPipeline.setCallerId(callerAddress.getId()).setCallerAddress(MessageTypes.toTypeName(callerAddress));
                }

                context.send(MessageTypes.toSdkAddress(rootPipelineAddress), MessageTypes.wrap(childPipeline.build()));
            }
        }
    }

    public void notifyPipelineStatusChanged(Context context, TaskStatus.Status status) {
        if (!configuration.hasEventsEgress()) {
            return;
        }

        var taskStatus = TaskStatus.newBuilder().setValue(status);
        var pipelineStatusChanged = PipelineStatusChanged.newBuilder().setStatus(taskStatus);
        var event = MessageTypes.buildEventFor(state).setPipelineStatusChanged(pipelineStatusChanged);
        context.send(MessageTypes.getEventsEgress(configuration), MessageTypes.toEgress(event.build(), configuration.getEventsTopic()));
    }
}
