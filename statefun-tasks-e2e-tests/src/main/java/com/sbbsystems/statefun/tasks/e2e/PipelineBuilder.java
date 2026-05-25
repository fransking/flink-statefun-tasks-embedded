/*
 * Copyright [2023] [Frans King, Luke Ashworth]
 * Copyright [2026] [Frans King]
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
package com.sbbsystems.statefun.tasks.e2e;

import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.generated.Pipeline;

/**
 * Convenience wrapper around {@link com.sbbsystems.statefun.tasks.pipeline.PipelineBuilder}
 * for use in e2e tests.  All task entries are pre-bound to
 * {@link EndToEndRemoteFunction#FUNCTION_TYPE} so callers only need to supply a task type.
 */
public class PipelineBuilder {

    private static com.sbbsystems.statefun.tasks.pipeline.PipelineBuilder forE2eWorker() {
        return com.sbbsystems.statefun.tasks.pipeline.PipelineBuilder.forWorker(
                EndToEndRemoteFunction.FUNCTION_TYPE.namespace(),
                EndToEndRemoteFunction.FUNCTION_TYPE.name());
    }

    public static com.sbbsystems.statefun.tasks.pipeline.PipelineBuilder forE2eWorker(boolean useLegacyTypes) {
        return com.sbbsystems.statefun.tasks.pipeline.PipelineBuilder.forWorker(
                EndToEndRemoteFunction.FUNCTION_TYPE.namespace(),
                useLegacyTypes
                        ? EndToEndRemoteFunction.FUNCTION_TYPE.name()
                        : EndToEndRemoteFunction.VALUE_FUNCTION_TYPE.name());
    }

    public static com.sbbsystems.statefun.tasks.pipeline.PipelineBuilder beginWith(String taskType) {
        return forE2eWorker().beginWith(taskType);
    }

    public static com.sbbsystems.statefun.tasks.pipeline.PipelineBuilder beginWith(String taskType, Message request) {
        return forE2eWorker().beginWith(taskType, request);
    }

    public static com.sbbsystems.statefun.tasks.pipeline.PipelineBuilder inParallel(Iterable<Pipeline> entries) {
        return forE2eWorker().inParallel(entries);
    }

    public static com.sbbsystems.statefun.tasks.pipeline.PipelineBuilder inParallel(Iterable<Pipeline> entries, boolean returnExceptions) {
        return forE2eWorker().inParallel(entries, returnExceptions);
    }

    public static com.sbbsystems.statefun.tasks.pipeline.PipelineBuilder inParallel(Iterable<Pipeline> entries, int maxParallelism) {
        return forE2eWorker().inParallel(entries, maxParallelism);
    }

    // not instantiable
    private PipelineBuilder() {}
}
