package com.sbbsystems.statefun.tasks.testmodule;

import com.google.protobuf.Any;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

public final class IoIdentifiers {

    public static final String NAMESPACE = "test-harness";
    public static final IngressIdentifier<TaskRequest> REQUEST_INGRESS = new IngressIdentifier<>(TaskRequest.class, NAMESPACE, "in");
    public static final EgressIdentifier<Any> RESULT_EGRESS = new EgressIdentifier<>(NAMESPACE, "out", Any.class);

    public static final FunctionType REMOTE_FUNCTION_TYPE = new FunctionType(NAMESPACE, "remote_function");
    public static final FunctionType EMBEDDED_FUNCTION_TYPE = new FunctionType(NAMESPACE, "embedded_function");
}
