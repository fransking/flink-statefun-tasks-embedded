package com.sbbsystems.statefun.tasks.events;

import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.graph.Entry;
import com.sbbsystems.statefun.tasks.graph.Group;
import com.sbbsystems.statefun.tasks.graph.PipelineGraph;
import com.sbbsystems.statefun.tasks.graph.Task;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.TimedBlock;
import org.apache.flink.statefun.sdk.Context;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static com.sbbsystems.statefun.tasks.util.MoreObjects.notEqualsAndNotNull;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class PipelineEvents {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineEvents.class);

    private final PipelineConfiguration configuration;
    private final PipelineFunctionState state;

    public static PipelineEvents from(@NotNull PipelineConfiguration configuration, @NotNull PipelineFunctionState state) {
        return new PipelineEvents(requireNonNull(configuration), requireNonNull(state));
    }

    private PipelineEvents(PipelineConfiguration configuration, PipelineFunctionState state) {
        this.configuration = configuration;
        this.state = state;
    }
    
    public void notifyPipelineCreated(Context context, PipelineGraph graph) {
        var pipelineAddress = state.getPipelineAddress();
        var rootPipelineAddress = state.getRootPipelineAddress();
        var callerAddress = state.getCallerAddress();

        try (var ignored = TimedBlock.of(LOG::info, "Notifying new pipeline {0} created", MessageTypes.asString(pipelineAddress))) {


            if (configuration.hasEventsEgress()) {
                // if we have events egress then publish PipelineCreated message
                var pipelineCreated = PipelineCreated.newBuilder().setPipeline(toPipelineInfo(graph));

                if (!isNull(callerAddress)) {
                    pipelineCreated.setCallerId(callerAddress.getId()).setCallerAddress(MessageTypes.toTypeName(callerAddress));
                }

                var event = MessageTypes.buildEventFor(state).setPipelineCreated(pipelineCreated);
                context.send(MessageTypes.getEventsEgress(configuration), MessageTypes.toEgress(event.build(), configuration.getEventsTopic()));
            }

            if (notEqualsAndNotNull(pipelineAddress, rootPipelineAddress)) {
                // if this is a child pipeline then notify the root pipeline of a new descendant
                var tasks = graph.getTaskEntries().map(MessageTypes::toTaskInfo).collect(Collectors.toUnmodifiableList());

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

    public void notifyPipelineTasksSkipped(Context context, PipelineGraph graph, List<Task> skippedTasks) {
        if (!configuration.hasEventsEgress() || skippedTasks.isEmpty()) {
            return;
        }

        var tasks = skippedTasks.stream()
                .map(t -> graph.getTaskEntry(t.getId()))
                .map(MessageTypes::toTaskInfo)
                .collect(Collectors.toUnmodifiableList());

        var tasksSkipped = PipelineTasksSkipped.newBuilder().addAllTasks(tasks);
        var event = MessageTypes.buildEventFor(state).setPipelineTasksSkipped(tasksSkipped);
        context.send(MessageTypes.getEventsEgress(configuration), MessageTypes.toEgress(event.build(), configuration.getEventsTopic()));
    }

    public void notifyPipelineTaskFinished(Context context, TaskResultOrException message) {
        if (!configuration.hasEventsEgress()) {
            return;
        }

        var taskFinished = PipelineTaskFinished.newBuilder()
                .setSizeInBytes(message.getSerializedSize());

        if (message.hasTaskResult()) {
            var taskResult = message.getTaskResult();

            taskFinished.setId(taskResult.getId())
                    .setUid(taskResult.getUid())
                    .setStatus(TaskStatus.newBuilder().setValue(TaskStatus.Status.COMPLETED));

        } else if (message.hasTaskException()) {
            var taskException = message.getTaskException();

            taskFinished.setId(taskException.getId())
                    .setUid(taskException.getUid())
                    .setStatus(TaskStatus.newBuilder().setValue(TaskStatus.Status.FAILED));
        }

        var event = MessageTypes.buildEventFor(state).setPipelineTaskFinished(taskFinished);
        context.send(MessageTypes.getEventsEgress(configuration), MessageTypes.toEgress(event.build(), configuration.getEventsTopic()));
    }

    private PipelineInfo.Builder toPipelineInfo(PipelineGraph graph) {
        var pipelineInfo = PipelineInfo.newBuilder();
        extractEntryInfo(graph.getHead(), graph, pipelineInfo);
        return pipelineInfo;
    }

    private void extractEntryInfo(Entry entry, PipelineGraph graph, PipelineInfo.Builder pipelineInfo) {
        while(!isNull(entry)) {

            if (entry instanceof Group) {
                var group = (Group) entry;
                var groupInfo = GroupInfo.newBuilder().setGroupId(group.getId());

                for (var item: group.getItems()) {
                    var groupPipelineInfo = PipelineInfo.newBuilder();
                    extractEntryInfo(item, graph, groupPipelineInfo);
                    groupInfo.addGroup(groupPipelineInfo);
                }

//                .setGroup(groupPipelineInfo);
                pipelineInfo.addEntries(EntryInfo.newBuilder().setGroupEntry(groupInfo));

            } else if (entry instanceof Task) {
                var taskEntry = graph.getTaskEntry(entry.getId());
                pipelineInfo.addEntries(EntryInfo.newBuilder().setTaskEntry(MessageTypes.toTaskInfo(taskEntry)));
            }

            entry = entry.getNext();
        }
    }
}
