package com.sbbsystems.statefun.tasks.benchmarking;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClientFactory;
import org.apache.flink.statefun.sdk.TypeName;

import java.net.URI;

public class NoopRequestReplyClientFactory implements RequestReplyClientFactory {
    public static TypeName KIND_TYPE = TypeName.parseFrom("io.statefun_tasks.transports.v1/noop");
    public static NoopRequestReplyClientFactory INSTANCE = new NoopRequestReplyClientFactory();

    @Override
    public RequestReplyClient createTransportClient(ObjectNode objectNode, URI uri) {
        return new NoopRequestReplyClient();
    }

    @Override
    public void cleanup() {
    }
}
