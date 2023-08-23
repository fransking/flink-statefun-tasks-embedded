package com.sbbsystems.statefun.tasks.benchmarking;

import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.TimedBlock;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.reqreply.generated.Address;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class NoopRequestReplyClient implements RequestReplyClient {
    private static final Logger LOG = LoggerFactory.getLogger(NoopRequestReplyClient.class);

    @Override
    public CompletableFuture<FromFunction> call(ToFunctionRequestSummary toFunctionRequestSummary, RemoteInvocationMetrics remoteInvocationMetrics, ToFunction toFunction) {
        return CompletableFuture.supplyAsync(() -> toFromFunction(toFunction));
    }

    private static FromFunction toFromFunction(ToFunction toFunction) {
        var fromFunction = FromFunction.newBuilder();

        try (var ignored = TimedBlock.of(LOG::info, "Processing noop ToFunction of {0} messages", toFunction.getInvocation().getInvocationsCount())) {

            for (var invocation : toFunction.getInvocation().getInvocationsList()) {
                var typedValue = invocation.getArgument();
                var response = FromFunction.InvocationResponse.newBuilder();

                if (MessageTypes.isType(typedValue, TaskRequest.class)) {
                    try {
                        var taskRequest = MessageTypes.asType(typedValue, TaskRequest::parseFrom);

                        var taskResult = TaskResult.newBuilder()
                                .setResult(MessageTypes.packAny(TupleOfAny.getDefaultInstance()))
                                .setId(taskRequest.getId())
                                .setUid(taskRequest.getUid())
                                .setInvocationId(taskRequest.getInvocationId());

                        var callbackAddress = Address.newBuilder()
                                .setNamespace(taskRequest.getReplyAddress().getNamespace())
                                .setType(taskRequest.getReplyAddress().getType())
                                .setId(taskRequest.getReplyAddress().getId());

                        response.addOutgoingMessages(FromFunction.Invocation.newBuilder()
                                .setArgument(MessageTypes.wrap(taskResult.build()))
                                .setTarget(callbackAddress));

                    } catch (Exception e) {
                        LOG.warn("Error handling TaskRequest", e);
                    }
                }

                fromFunction.setInvocationResult(response);
            }

            return fromFunction.build();

        }
    }
}
