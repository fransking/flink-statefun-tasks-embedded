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

public final class TaskEntry {
    public String taskId;
    public String taskType;
    public byte[] request;
    public boolean isFinally;
    public String namespace;
    public String workerName;
    public boolean isFruitful;
    public RetryPolicy retryPolicy;
    public String displayName;
    public boolean isWait;
    public String uid;
    public boolean isExceptionally;
}
