package com.sbbsystems.flink.tasks.embedded.testmodule;

import com.google.protobuf.Any;
import com.sbbsystems.flink.tasks.embedded.proto.TaskRequest;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;

public final class IoIdentifiers {
    public static final EgressIdentifier<Any> RESULT_EGRESS = new EgressIdentifier<>("example", "out", Any.class);

    public static final IngressIdentifier<TaskRequest> REQUEST_INGRESS = new IngressIdentifier<>(TaskRequest.class, "exmaple", "in");

    public static final FunctionType REMOTE_FUNCTION_TYPE = new FunctionType("example", "remote_function");
    public static final FunctionType EMBEDDED_FUNCTION_TYPE = new FunctionType("example", "embedded_function");
}
