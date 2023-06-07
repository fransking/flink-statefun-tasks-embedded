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

import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;

public class EchoFunction implements StatefulFunction {

    @Override
    public void invoke(Context context, Object input) {
        try {
            if (MessageTypes.isType(input, TaskRequest.class)) {
                var taskRequest = MessageTypes.asType(input, TaskRequest::parseFrom);
                var taskResult = TaskResult.newBuilder().setId(taskRequest.getId())
                        .setUid(taskRequest.getUid())
                        .setInvocationId(taskRequest.getInvocationId())
                        .setType(taskRequest.getType() + ".result")
                        .setResult(taskRequest.getRequest())
                        .build();
                var wrappedResult = MessageTypes.wrap(taskResult);
                var replyAddress = taskRequest.getReplyAddress();
                context.send(new FunctionType(replyAddress.getNamespace(), replyAddress.getType()), replyAddress.getId(), wrappedResult);
            }
        } catch (InvalidMessageTypeException e) {
            throw new RuntimeException(e);
        }
    }
}
