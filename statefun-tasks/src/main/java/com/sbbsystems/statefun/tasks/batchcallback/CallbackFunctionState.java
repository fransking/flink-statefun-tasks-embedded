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

package com.sbbsystems.statefun.tasks.batchcallback;

import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedAppendingBuffer;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public class CallbackFunctionState {
    @Persisted
    private final PersistedAppendingBuffer<TaskResultOrException> batch = PersistedAppendingBuffer.of("batch", TaskResultOrException.class);

    @Persisted
    private final PersistedValue<Boolean> pipelineRequestInProgress = PersistedValue.of("pipelineRequestInProgress", Boolean.class);

    private CallbackFunctionState() {
    }

    public static CallbackFunctionState newInstance() {
        return new CallbackFunctionState();
    }

    public PersistedAppendingBuffer<TaskResultOrException> getBatch() {
        return batch;
    }

    public PersistedValue<Boolean> getPipelineRequestInProgress() {
        return pipelineRequestInProgress;
    }
}
