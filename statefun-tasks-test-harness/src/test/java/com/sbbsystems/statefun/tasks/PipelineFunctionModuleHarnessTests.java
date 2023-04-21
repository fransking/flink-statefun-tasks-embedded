package com.sbbsystems.statefun.tasks;

import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;
import com.sbbsystems.statefun.tasks.generated.ArgsAndKwargs;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;
import com.sbbsystems.statefun.tasks.testmodule.IoIdentifiers;
import com.sbbsystems.statefun.tasks.utils.HarnessUtils;
import com.sbbsystems.statefun.tasks.utils.IngressMessageSupplier;
import org.apache.flink.statefun.flink.harness.Harness;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

import static org.assertj.core.api.Assertions.assertThat;


public class PipelineFunctionModuleHarnessTests {
    static LinkedBlockingDeque<Any> egressMessages = new LinkedBlockingDeque<>();

    @Test
    void testCallingAddTask() throws Exception {
        var taskRequest = TaskRequest.newBuilder()
                .setId("123")
                .setUid("456")
                .setReplyTopic(IoIdentifiers.RESULT_EGRESS.name())
                .setType("add")
                .setRequest(Any.pack(
                        ArgsAndKwargs.newBuilder()
                                .setArgs(
                                        TupleOfAny.newBuilder()
                                                .addItems(Any.pack(Int32Value.of(1)))
                                                .addItems(Any.pack(Int32Value.of(1)))
                                                .build())
                                .build()))
                .build();
        var ingress = IngressMessageSupplier.create(List.of(
                taskRequest
        ));
        var harness = new Harness()
                .withParallelism(1)
                .withSupplyingIngress(IoIdentifiers.REQUEST_INGRESS, ingress)
                .withConsumingEgress(IoIdentifiers.RESULT_EGRESS, msg -> egressMessages.add(msg));

        Any result;
        try (AutoCloseable ignored = HarnessUtils.startHarnessInTheBackground(harness)) {
            result = egressMessages.take();
        }

        int unpackedResult = result.unpack(TaskResult.class).getResult().unpack(Int32Value.class).getValue();
        assertThat(unpackedResult).isEqualTo(2);
    }
}
