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

import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.TaskRequest;

public final class TaskRequestSerializer {
    private final TaskRequest taskRequest;

    public static TaskRequestSerializer of(TaskRequest taskRequest) {
        return new TaskRequestSerializer(taskRequest);
    }

    private TaskRequestSerializer(TaskRequest taskRequest) {
        this.taskRequest = taskRequest;
    }

    public ArgsAndKwargsSerializer getArgsAndKwargsSerializer()
            throws StatefunTasksException {

        return ArgsAndKwargsSerializer.of(taskRequest.getRequest());
    }
}
