/*
 * Copyright [2023] [Frans King, Luke Ashworth]
 * Copyright [2026] [Frans King]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sbbsystems.statefun.tasks;

import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;
import com.sbbsystems.statefun.tasks.generated.*;
import com.sbbsystems.statefun.tasks.testmodule.IoIdentifiers;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;


public class PipelineFunctionModuleHarnessTests {
    private NamespacedTestHarness legacyHarness;
    private NamespacedTestHarness valueHarness;

    @BeforeEach
    void setup() {
        legacyHarness = NamespacedTestHarness.newInstance(true);
        valueHarness = NamespacedTestHarness.newInstance(false);
    }

    // ---- legacy tests -----------------------------------------------------------------------

    @Test
    void returns_result_for_single_task() throws Exception {
        var taskEntry = TaskEntry.newBuilder()
                .setNamespace(IoIdentifiers.ECHO_FUNCTION_TYPE.namespace())
                .setWorkerName(IoIdentifiers.ECHO_FUNCTION_TYPE.name())
                .setRequest(Any.pack(Int32Value.of(1)))
                .setTaskId("1")
                .setUid("1")
                .build();
        var pipeline = Pipeline.newBuilder()
                .addEntries(PipelineEntry.newBuilder().setTaskEntry(taskEntry))
                .build();

        var result = legacyHarness.runPipeline(pipeline);

        assertThat(result.getTypename()).isEqualTo("type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord");
        var kafkaProducerRecord = KafkaProducerRecord.parseFrom(result.getValue());
        var egressValue = Any.parseFrom(kafkaProducerRecord.getValueBytes());
        assertThat(egressValue.is(TaskResult.class)).isTrue();
        assertThat(egressValue.unpack(TaskResult.class).getResult().is(ArgsAndKwargs.class)).isTrue();
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
                .map(taskEntry -> PipelineEntry.newBuilder().setTaskEntry(taskEntry).build())
                .collect(Collectors.toList());

        var pipeline = Pipeline.newBuilder().addAllEntries(pipelineEntries).build();

        var result = legacyHarness.runPipeline(pipeline);

        assertThat(result.getTypename()).isEqualTo("type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord");
        var kafkaProducerRecord = KafkaProducerRecord.parseFrom(result.getValue());
        var egressValue = Any.parseFrom(kafkaProducerRecord.getValueBytes());
        assertThat(egressValue.is(TaskResult.class)).isTrue();
        assertThat(egressValue.unpack(TaskResult.class).getResult().is(ArgsAndKwargs.class)).isTrue();
    }

    // ---- value type tests -------------------------------------------------------------------

    private TaskEntry valueTaskEntry(String id, Value argValue) {
        return TaskEntry.newBuilder()
                .setNamespace(IoIdentifiers.ECHO_FUNCTION_TYPE.namespace())
                .setWorkerName(IoIdentifiers.ECHO_FUNCTION_TYPE.name())
                .setRequest(Any.pack(argValue))
                .setTaskId(id)
                .setUid(id)
                .build();
    }

    @Test
    void returns_result_for_single_task_value_mode() throws Exception {
        var pipeline = Pipeline.newBuilder()
                .addEntries(PipelineEntry.newBuilder()
                        .setTaskEntry(valueTaskEntry("1", Value.newBuilder().setIntValue(1).build())))
                .build();

        var result = valueHarness.runPipeline(pipeline);

        assertThat(result.getTypename()).isEqualTo("type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord");
        var kafkaProducerRecord = KafkaProducerRecord.parseFrom(result.getValue());
        var egressValue = Any.parseFrom(kafkaProducerRecord.getValueBytes());
        assertThat(egressValue.is(TaskResult.class)).isTrue();
        assertThat(egressValue.unpack(TaskResult.class).getResult().is(ValueArgsAndKwargs.class)).isTrue();
    }

    @Test
    void returns_result_for_single_chain_value_mode() throws Exception {
        var pipelineEntries = IntStream.range(0, 3)
                .boxed()
                .map(i -> PipelineEntry.newBuilder()
                        .setTaskEntry(valueTaskEntry(i.toString(), Value.newBuilder().setIntValue(i).build()))
                        .build())
                .collect(Collectors.toList());

        var pipeline = Pipeline.newBuilder().addAllEntries(pipelineEntries).build();

        var result = valueHarness.runPipeline(pipeline);

        assertThat(result.getTypename()).isEqualTo("type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord");
        var kafkaProducerRecord = KafkaProducerRecord.parseFrom(result.getValue());
        var egressValue = Any.parseFrom(kafkaProducerRecord.getValueBytes());
        assertThat(egressValue.is(TaskResult.class)).isTrue();
        assertThat(egressValue.unpack(TaskResult.class).getResult().is(ValueArgsAndKwargs.class)).isTrue();
    }
}