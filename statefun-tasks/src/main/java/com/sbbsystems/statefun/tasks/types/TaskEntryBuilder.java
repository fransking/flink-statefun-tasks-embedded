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

public final class TaskEntryBuilder {
    public static TaskEntry fromProto(com.sbbsystems.statefun.tasks.generated.TaskEntry taskEntry) {
        var task = new TaskEntry();

        task.taskId = taskEntry.getTaskId();
        task.taskType = taskEntry.getTaskType();
        task.request = taskEntry.getRequest().toByteArray();
        task.isFinally = taskEntry.getIsFinally();
        task.namespace = taskEntry.getNamespace();
        task.workerName = taskEntry.getWorkerName();
        task.isFruitful = taskEntry.getIsFruitful();

        if (taskEntry.hasRetryPolicy()) {
            task.retryPolicy = RetryPolicyBuilder.fromProto(taskEntry.getRetryPolicy());
        }

        task.displayName = taskEntry.getDisplayName();
        task.isWait = taskEntry.getIsWait();
        task.uid = taskEntry.getUid();
        task.isExceptionally = taskEntry.getIsExceptionally();

        return task;
    }
}
