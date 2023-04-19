package com.sbbsystems.flink.tasks.embedded.testmodule;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.sbbsystems.flink.tasks.embedded.proto.TaskRequest;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;

import static com.sbbsystems.flink.tasks.embedded.testmodule.IoIdentifiers.RESULT_EGRESS;

/**
 * Temporary implementation of the pipeline function to verify messages can be sent to other functions and egress
 **/
public class TempPipelineFunction implements StatefulFunction {

    @Override
    public void invoke(Context context, Object o) {
        var taskRequest = (TaskRequest)o;
        String request;
        try {
            request = taskRequest.getRequest().unpack(StringValue.class).getValue();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        if (request.length() <= 1) {
            context.send(RESULT_EGRESS, Any.pack(StringValue.of(request)));
        } else {
            context.send(new FunctionType("example", "remote_function"), taskRequest.getId(), taskRequest);
        }
    }
}
