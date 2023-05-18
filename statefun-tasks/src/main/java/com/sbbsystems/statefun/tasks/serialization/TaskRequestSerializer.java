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
package com.sbbsystems.statefun.tasks.serialization;

import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.ArgsAndKwargs;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;

import java.nio.channels.Pipe;

public final class TaskRequestSerializer {
    private final TaskRequest taskRequest;

    public static TaskRequestSerializer forRequest(TaskRequest taskRequest) {
        return new TaskRequestSerializer(taskRequest);
    }

    private TaskRequestSerializer(TaskRequest taskRequest) {
        this.taskRequest = taskRequest;
    }

    public ArgsAndKwargsSerializer getArgsAndKwargs()
            throws StatefunTasksException {

        try {
            var request = taskRequest.getRequest();

            if (request.is(ArgsAndKwargs.class)) {
                return ArgsAndKwargsSerializer.from(request.unpack(ArgsAndKwargs.class));
            }
            else {
                return ArgsAndKwargsSerializer.from(ArgsAndKwargs
                        .newBuilder()
                        .setArgs(TupleOfAny.newBuilder().addItems(request).build())
                        .build());
            }

        } catch (InvalidProtocolBufferException e) {
            throw new InvalidMessageTypeException("Protobuf parsing error", e);
        }
    }
}
