package com.sbbsystems.flink.tasks.embedded.types;

import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.flink.tasks.embedded.proto.TaskException;
import com.sbbsystems.flink.tasks.embedded.proto.TaskRequest;
import com.sbbsystems.flink.tasks.embedded.proto.TaskResult;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;


public class MessageTypesTests {
    @Test
    public void isType_returns_correct_type_for_task_request() {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.TaskRequest").build();
        assertTrue(MessageTypes.isType(typedValue, TaskRequest.class));
    }

    @Test
    public void isType_returns_correct_type_for_task_result() {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.TaskResult").build();
        assertTrue(MessageTypes.isType(typedValue, TaskResult.class));
    }

    @Test
    public void isType_returns_correct_type_for_task_exception() {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.TaskException").build();
        assertTrue(MessageTypes.isType(typedValue, TaskException.class));
    }

    @Test
    public void asType_returns_correct_type_for_task_request() throws InvalidMessageTypeException {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.TaskRequest").build();
        assertInstanceOf(TaskRequest.class, MessageTypes.asType(typedValue, TaskRequest::parseFrom));
    }

    @Test
    public void asType_returns_correct_type_for_task_result() throws InvalidMessageTypeException {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.TaskResult").build();
        assertInstanceOf(TaskResult.class, MessageTypes.asType(typedValue, TaskResult::parseFrom));
    }

    @Test
    public void asType_returns_correct_type_for_task_exception() throws InvalidMessageTypeException {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.TaskException").build();
        assertInstanceOf(TaskException.class, MessageTypes.asType(typedValue, TaskException::parseFrom));
    }
}
