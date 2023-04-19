package com.sbbsystems.statefun.tasks;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.utils.IngressMessageSupplier;
import com.sbbsystems.statefun.tasks.testmodule.IoIdentifiers;
import com.sbbsystems.statefun.tasks.utils.HarnessUtils;
import org.apache.flink.statefun.flink.harness.Harness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

import static org.apache.flink.util.function.FunctionUtils.uncheckedFunction;
import static org.assertj.core.api.Assertions.assertThat;


public class PipelineFunctionModuleHarnessTests {
    static LinkedBlockingDeque<Any> egressMessages = new LinkedBlockingDeque<>();

    @Test
    void testSendingMessages() throws Exception {
        var ingress = IngressMessageSupplier.create(List.of(
                TaskRequest.newBuilder().setRequest(Any.pack(StringValue.of("hello"))).build(),
                TaskRequest.newBuilder().setRequest(Any.pack(StringValue.of("world"))).build()
        ));
        var harness = new Harness()
                .withParallelism(1)
                .withSupplyingIngress(IoIdentifiers.REQUEST_INGRESS, ingress)
                .withConsumingEgress(IoIdentifiers.RESULT_EGRESS, msg -> egressMessages.add(msg));

        List<Any> result = new ArrayList<>();
        try (AutoCloseable ignored = HarnessUtils.startHarnessInTheBackground(harness)) {
            result.add(egressMessages.take());
            result.add(egressMessages.take());
        }

        var resultStrings = result.stream()
                .map(uncheckedFunction((Any any) -> any.unpack(StringValue.class).getValue()))
                .collect(Collectors.toList());

        assertThat(resultStrings).containsExactlyInAnyOrder("h", "w");
    }
}
