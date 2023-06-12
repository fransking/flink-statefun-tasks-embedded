package com.sbbsystems.statefun.tasks.e2e;

import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.generated.MapOfStringToAny;
import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.PipelineEntry;
import com.sbbsystems.statefun.tasks.generated.TaskEntry;
import com.sbbsystems.statefun.tasks.util.Id;

import static com.sbbsystems.statefun.tasks.types.MessageTypes.packAny;

public class PipelineBuilder {

    private final Pipeline.Builder pipeline;

    public static PipelineBuilder beginWith(String taskType, Message request) {
        return new PipelineBuilder().addTask(taskType, request);
    }

    private PipelineBuilder() {
        pipeline = Pipeline.newBuilder();
    }

    public Pipeline build() {
        return pipeline.build();
    }

    public PipelineBuilder withInitialState(Message initialState) {
        pipeline.setInitialState(packAny(initialState));
        return this;
    }

    public PipelineBuilder withInitialArgs(Message initialArgs) {
        pipeline.setInitialArgs(packAny(initialArgs));
        return this;
    }

    public PipelineBuilder withInitialKwargs(MapOfStringToAny initialKwargs) {
        pipeline.setInitialKwargs(initialKwargs);
        return this;
    }

    public PipelineBuilder inline() {
        pipeline.setInline(true);
        return this;
    }

    private PipelineBuilder addTask(String taskType, Message request) {
        var taskEntry = TaskEntry.newBuilder()
                .setNamespace(RemoteFunction.FUNCTION_TYPE.namespace())
                .setWorkerName(RemoteFunction.FUNCTION_TYPE.name())
                .setTaskType(taskType)
                .setRequest(packAny(request))
                .setTaskId(Id.generate())
                .setUid(Id.generate())
                .build();
        var pipelineEntry = PipelineEntry.newBuilder()
                .setTaskEntry(taskEntry)
                .build();
        pipeline.addEntries(pipelineEntry);
        return this;
    }
}
