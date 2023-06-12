package com.sbbsystems.statefun.tasks.e2e;

import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.sbbsystems.statefun.tasks.e2e.MoreStrings.asString;
import static com.sbbsystems.statefun.tasks.types.MessageTypes.packAny;
import static org.assertj.core.api.Assertions.assertThat;


public class SimplePipelineTests {
    private NamespacedTestHarness harness;

    @BeforeEach
    void setup() {
        harness = NamespacedTestHarness.newInstance();
    }

    @Test
    void test_single_task_pipeline_returns_result() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(1))
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);

        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1)");
    }

    @Test
    void test_single_task_pipeline_returns_result_and_pipeline_state_when_inline() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("updateAndGetState", Int32Value.of(1))
                .withInitialState(Int32Value.of(2))
                .inline()
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline, packAny(Int32Value.of(100)));
        var taskResult = response.unpack(TaskResult.class);

        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("(3)");
        assertThat(state).isEqualTo("3");
    }

    @Test
    void test_single_task_pipeline_returns_result_and_task_state_when_not_inline() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("updateAndGetState", Int32Value.of(1))
                .withInitialState(Int32Value.of(2))
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline, packAny(Int32Value.of(100)));
        var taskResult = response.unpack(TaskResult.class);

        var result = asString(taskResult.getResult());
        var state = asString(taskResult.getState());

        assertThat(result).isEqualTo("(3)");
        assertThat(state).isEqualTo("100");
    }
}