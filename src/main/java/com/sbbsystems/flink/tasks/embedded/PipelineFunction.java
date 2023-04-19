package com.sbbsystems.flink.tasks.embedded;

import com.sbbsystems.flink.tasks.embedded.proto.TaskRequest;
import com.sbbsystems.flink.tasks.embedded.types.InvalidMessageTypeException;
import com.sbbsystems.flink.tasks.embedded.types.MessageTypes;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineFunction implements StatefulFunction {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineFunctionModule.class);

    @Override
    public void invoke(Context context, Object input) {
        try {
            if (MessageTypes.isType(input, TaskRequest.class)) {
                LOG.info("GOT A TASK_REQUEST_TYPE");

                var taskRequest = MessageTypes.asType(input, TaskRequest::parseFrom);
                LOG.info("TASK_REQUEST_ID is {}", taskRequest.getId());
            }
        }
        catch (InvalidMessageTypeException e) {
            LOG.error("Unable to handle input message", e);
        }
    }
}
