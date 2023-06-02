package com.sbbsystems.statefun.tasks;

import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;
import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.PipelineEntry;
import com.sbbsystems.statefun.tasks.generated.TaskEntry;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.testmodule.IoIdentifiers;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;


public class PipelineFunctionModuleHarnessTests {
    private NamespacedTestHarness harness;

    @BeforeEach
    void setup() {
        harness = NamespacedTestHarness.newInstance();
    }

    @Test
    void returns_result_for_single_task() throws Exception {
        var taskEntry = TaskEntry.newBuilder()
                .setNamespace(IoIdentifiers.ECHO_FUNCTION_TYPE.namespace())
                .setWorkerName(IoIdentifiers.ECHO_FUNCTION_TYPE.name())
                .setRequest(Any.pack(Int32Value.of(1)))
                .setTaskId("1")
                .setUid("1")
                .build();
        var pipelineEntry = PipelineEntry.newBuilder()
                .setTaskEntry(taskEntry)
                .build();
        var pipeline = Pipeline.newBuilder()
                .addEntries(pipelineEntry)
                .build();

        var result = harness.runPipeline(pipeline);

        assertThat(result.getTypename()).isEqualTo("type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord");
        var kafkaProducerRecord = KafkaProducerRecord.parseFrom(result.getValue());
        var egressValue = Any.parseFrom(kafkaProducerRecord.getValueBytes());
        assertThat(egressValue.is(TaskResult.class)).isTrue();
    }

    @Test
    void returns_result_for_single_chain() throws Exception {
        var pipelineEntries = IntStream.range(0, 3)
                .boxed()
                .map(i -> TaskEntry.newBuilder()
                        .setNamespace(IoIdentifiers.ECHO_FUNCTION_TYPE.namespace())
                        .setWorkerName(IoIdentifiers.ECHO_FUNCTION_TYPE.name())
                        .setRequest(Any.pack(Int32Value.of(1)))
                        .setTaskId(i.toString())
                        .setUid(i.toString())
                        .build())
                .map(taskEntry -> PipelineEntry.newBuilder()
                        .setTaskEntry(taskEntry)
                        .build())
                .collect(Collectors.toList());

        var pipeline = Pipeline.newBuilder()
                .addAllEntries(pipelineEntries)
                .build();

        var result = harness.runPipeline(pipeline);

        assertThat(result.getTypename()).isEqualTo("type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord");
        var kafkaProducerRecord = KafkaProducerRecord.parseFrom(result.getValue());
        var egressValue = Any.parseFrom(kafkaProducerRecord.getValueBytes());
        assertThat(egressValue.is(TaskResult.class)).isTrue();
    }
}
