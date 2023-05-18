/*
 * Copyright [2023] [Frans King, Luke Ashworth]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sbbsystems.statefun.tasks.types;

import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.*;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


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
    public void isType_returns_correct_type_for_callback_signal() {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.CallbackSignal").build();
        assertTrue(MessageTypes.isType(typedValue, CallbackSignal.class));
    }

    @Test
    public void isType_returns_correct_type_for_results_batch() {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.ResultsBatch").build();
        assertTrue(MessageTypes.isType(typedValue, ResultsBatch.class));
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

    @Test
    public void asType_returns_correct_type_for_callback_signal() throws InvalidMessageTypeException {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.CallbackSignal").build();
        assertInstanceOf(CallbackSignal.class, MessageTypes.asType(typedValue, CallbackSignal::parseFrom));
    }

    @Test
    public void asType_returns_correct_type_for_results_batch() throws InvalidMessageTypeException {
        var typedValue = TypedValue.newBuilder().setTypename("io.statefun_tasks.types/statefun_tasks.ResultsBatch").build();
        assertInstanceOf(ResultsBatch.class, MessageTypes.asType(typedValue, ResultsBatch::parseFrom));
    }

    @Test
    public void wrap_returns_typed_value_containing_argument() throws InvalidProtocolBufferException {
        var innerVal = TaskResult.newBuilder().setId("id").build();

        var typedVal = MessageTypes.wrap(innerVal);

        assertTrue(typedVal.getHasValue());
        assertEquals("io.statefun_tasks.types/statefun_tasks.TaskResult", typedVal.getTypename());
        assertEquals("id", TaskResult.parseFrom(typedVal.getValue()).getId());
    }
}
