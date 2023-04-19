package com.sbbsystems.statefun.tasks.testmodule;

import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.io.Router;

public class TestPipelineRouter implements Router<TaskRequest> {
    @Override
    public void route(TaskRequest request, Downstream<TaskRequest> downstream) {
        downstream.forward(new Address(IoIdentifiers.REMOTE_FUNCTION_TYPE, request.getId()), request);
    }
}
