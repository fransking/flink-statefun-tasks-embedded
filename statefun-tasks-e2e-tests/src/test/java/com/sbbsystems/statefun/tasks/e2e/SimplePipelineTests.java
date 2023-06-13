package com.sbbsystems.statefun.tasks.e2e;

import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.ArgsAndKwargs;
import com.sbbsystems.statefun.tasks.generated.MapOfStringToAny;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.apache.flink.kinesis.shaded.org.apache.http.util.Args;
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
    void test_pipeline_returns_result() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(1))
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1)");
    }

    @Test
    void test_args_are_sent_to_tasks() throws InvalidProtocolBufferException {
        var arguments = TupleOfAny
                .newBuilder()
                .addItems(packAny(Int32Value.of(1)))
                .addItems(packAny(Int32Value.of(2)))
                .build();

        var pipeline = PipelineBuilder
                .beginWith("echo", arguments)
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1, 2)");
    }

    @Test
    void test_args_and_kwargs_are_sent_to_tasks() throws InvalidProtocolBufferException {
        var args = TupleOfAny
                .newBuilder()
                .addItems(packAny(Int32Value.of(1)))
                .addItems(packAny(Int32Value.of(2)))
                .build();

        var kwargs = MapOfStringToAny
                .newBuilder()
                .putItems("k1", packAny(Int32Value.of(1)))
                .putItems("k2", packAny(Int32Value.of(2)))
                .build();

        var argsAndKwargs = ArgsAndKwargs
                .newBuilder()
                .setArgs(args)
                .setKwargs(kwargs)
                .build();

        var pipeline = PipelineBuilder
                .beginWith("echo", argsAndKwargs)
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1, 2, {k1: 1, k2: 2})");
    }

    @Test
    void test_pipeline_returns_result_and_pipeline_state_when_inline() throws InvalidProtocolBufferException {
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
    void test_pipeline_returns_result_and_task_state_when_not_inline() throws InvalidProtocolBufferException {
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

    @Test
    void test_initial_args_are_sent_to_initial_tasks() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(2))
                .withInitialArgs(TupleOfAny.newBuilder().addItems(packAny(Int32Value.of(1))).build())
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1, 2)");
    }

    @Test
    void test_initial_kwargs_are_sent_to_initial_tasks() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder
                .beginWith("echo", Int32Value.of(2))
                .withInitialKwargs(MapOfStringToAny.newBuilder().putItems("k1", packAny(Int32Value.of(3))).build())
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(2, {k1: 3})");
    }

    @Test
    void test_task_results_are_sent_to_continuations() throws InvalidProtocolBufferException {
        var arguments = TupleOfAny
                .newBuilder()
                .addItems(packAny(Int32Value.of(1)))
                .addItems(packAny(Int32Value.of(2)))
                .build();

        var pipeline = PipelineBuilder
                .beginWith("echo", arguments)
                .continueWith("echo")
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var taskResult = response.unpack(TaskResult.class);
        var result = asString(taskResult.getResult());

        assertThat(result).isEqualTo("(1, 2)");
    }
}