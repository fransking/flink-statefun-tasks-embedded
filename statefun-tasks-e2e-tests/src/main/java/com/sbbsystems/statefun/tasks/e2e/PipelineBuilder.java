package com.sbbsystems.statefun.tasks.e2e;

import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.generated.MapOfStringToAny;
import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.PipelineEntry;
import com.sbbsystems.statefun.tasks.generated.TaskEntry;
import com.sbbsystems.statefun.tasks.util.Id;

import java.util.Objects;

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

    public PipelineBuilder continueWith(String taskType) {
        return continueWith(taskType, null);
    }

    public PipelineBuilder continueWith(String taskType, Message request) {
        return this.addTask(taskType, request);
    }

    private PipelineBuilder addTask(String taskType, Message request) {
        var taskEntry = TaskEntry.newBuilder()
                .setNamespace(EndToEndRemoteFunction.FUNCTION_TYPE.namespace())
                .setWorkerName(EndToEndRemoteFunction.FUNCTION_TYPE.name())
                .setTaskType(taskType)
                .setTaskId(Id.generate())
                .setUid(Id.generate());

        if (!Objects.isNull(request)) {
            taskEntry.setRequest(packAny(request));
        }

        var pipelineEntry = PipelineEntry.newBuilder()
                .setTaskEntry(taskEntry)
                .build();
        pipeline.addEntries(pipelineEntry);
        return this;
    }
}
