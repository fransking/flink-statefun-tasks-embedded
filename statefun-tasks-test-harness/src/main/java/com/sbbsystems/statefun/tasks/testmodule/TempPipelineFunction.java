package com.sbbsystems.statefun.tasks.testmodule;

import com.google.protobuf.Any;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

/**
 * Temporary implementation of the pipeline function to verify messages can be sent to other functions and egress
 **/
public class TempPipelineFunction implements StatefulFunction {

    @Override
    public void invoke(Context context, Object o) {
        if (o instanceof TaskRequest) {
            var taskRequest = (TaskRequest)o;
            context.send(IoIdentifiers.REMOTE_FUNCTION_TYPE, taskRequest.getId(), taskRequest);
        }
        else if (o instanceof TaskResult) {
            context.send(IoIdentifiers.RESULT_EGRESS, Any.pack((TaskResult)o));
        }
        else {
            throw new RuntimeException(String.format("Unexpected message type %s", o.getClass().getTypeName()));
        }
    }
}
