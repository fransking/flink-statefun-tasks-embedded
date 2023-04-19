package com.sbbsystems.flink.tasks.embedded.testmodule;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.sbbsystems.flink.tasks.embedded.proto.TaskRequest;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

import static com.sbbsystems.flink.tasks.embedded.testmodule.IoIdentifiers.EMBEDDED_FUNCTION_TYPE;

public class TestRemoteFunction implements StatefulFunction {
    @Override
    public void invoke(Context context, Object o) {
        var taskRequest = (TaskRequest)o;
        String request;
        try {
            request = taskRequest.getRequest().unpack(StringValue.class).getValue();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        String undatedRequest = request.substring(0, request.length() - 1);
        var updated = taskRequest.toBuilder().setRequest(Any.pack(StringValue.of(undatedRequest))).build();
        context.send(EMBEDDED_FUNCTION_TYPE, updated.getId(), updated);
    }
}
