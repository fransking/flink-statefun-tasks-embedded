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
package com.sbbsystems.statefun.tasks.testmodule;

import com.sbbsystems.statefun.tasks.generated.TaskActionRequest;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.Router;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public class TestPipelineRouter implements Router<TypedValue> {
    @Override
    public void route(TypedValue request, Downstream<TypedValue> downstream) {
        String requestId;
        try {
            if (MessageTypes.isType(request, TaskRequest.class)) {
                var taskRequest = MessageTypes.asType(request, TaskRequest::parseFrom);
                requestId = taskRequest.getId();
            } else if (MessageTypes.isType(request, TaskActionRequest.class)) {
                var actionRequest = MessageTypes.asType(request, TaskActionRequest::parseFrom);
                requestId = actionRequest.getId();
            } else {
                throw new RuntimeException("Unexpected message type: " + request.getTypename());
            }
        } catch (InvalidMessageTypeException e) {
            throw new RuntimeException(e);
        }
        var embeddedPipelineFunctionType = new FunctionType("example", "embedded_pipeline");
        downstream.forward(new Address(embeddedPipelineFunctionType, requestId), request);
    }
}
