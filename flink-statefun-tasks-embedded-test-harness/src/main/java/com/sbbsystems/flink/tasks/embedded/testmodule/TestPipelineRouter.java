package com.sbbsystems.flink.tasks.embedded.testmodule;

import com.sbbsystems.flink.tasks.embedded.generated.TaskRequest;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.io.Router;

import static com.sbbsystems.flink.tasks.embedded.testmodule.IoIdentifiers.REMOTE_FUNCTION_TYPE;

public class TestPipelineRouter implements Router<TaskRequest> {
    @Override
    public void route(TaskRequest request, Downstream<TaskRequest> downstream) {
        downstream.forward(new Address(REMOTE_FUNCTION_TYPE, request.getId()), request);
    }
}
