package com.sbbsystems.flink.tasks.embedded.testmodule;

import com.sbbsystems.flink.tasks.embedded.proto.TaskRequest;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.Router;

public class TestPipelineRouter implements Router<TaskRequest> {
    @Override
    public void route(TaskRequest request, Downstream<TaskRequest> downstream) {
        downstream.forward(new Address(new FunctionType("example", "remote_function"), request.getId()), request);
    }
}
