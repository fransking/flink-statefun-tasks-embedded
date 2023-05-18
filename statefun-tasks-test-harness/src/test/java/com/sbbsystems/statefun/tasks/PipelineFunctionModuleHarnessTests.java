package com.sbbsystems.statefun.tasks;

import com.google.protobuf.Any;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.testmodule.IoIdentifiers;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.utils.EgressMessageContainer;
import com.sbbsystems.statefun.tasks.utils.HarnessUtils;
import com.sbbsystems.statefun.tasks.utils.IngressMessageSupplier;
import org.apache.flink.statefun.flink.harness.Harness;
import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


public class PipelineFunctionModuleHarnessTests {
    @Test
    void testCallingSingleTask() throws Exception {
        var taskRequest = TaskRequest.newBuilder()
                .setId("123")
                .setUid("456")
                .setReplyTopic(IoIdentifiers.RESULT_EGRESS.name())
                .build();

        var ingress = IngressMessageSupplier.create(List.of(
                MessageTypes.wrap(taskRequest)
        ));

        var egress = new EgressMessageContainer<TypedValue>();
        var harness = new Harness()
                .withParallelism(1)
                .withSupplyingIngress(IoIdentifiers.REQUEST_INGRESS, ingress)
                .withConsumingEgress(IoIdentifiers.RESULT_EGRESS, egress::addMessage);

        var harnessThread = HarnessUtils.startHarnessInTheBackground(harness);
        TypedValue result;
        try (AutoCloseable ignored = harnessThread::interrupt) {
            result = egress.getMessage(harnessThread);
        }

        assertThat(result.getTypename()).isEqualTo("type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord");
        var kafkaProducerRecord = KafkaProducerRecord.parseFrom(result.getValue());
        var taskResult = Any.parseFrom(kafkaProducerRecord.getValueBytes()).unpack(TaskResult.class);
        Assertions.assertEquals("123", taskResult.getId());
    }

}
