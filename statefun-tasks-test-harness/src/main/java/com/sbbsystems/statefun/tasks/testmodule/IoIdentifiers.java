package com.sbbsystems.statefun.tasks.testmodule;

import com.google.protobuf.Any;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

public final class IoIdentifiers {
    public static final EgressIdentifier<Any> RESULT_EGRESS = new EgressIdentifier<>("test-harness", "out", Any.class);

    public static final IngressIdentifier<TaskRequest> REQUEST_INGRESS = new IngressIdentifier<>(TaskRequest.class, "test-harness", "in");

    public static final FunctionType REMOTE_FUNCTION_TYPE = new FunctionType("test-harness", "remote_function");
    public static final FunctionType EMBEDDED_FUNCTION_TYPE = new FunctionType("test-harness", "embedded_function");
}
