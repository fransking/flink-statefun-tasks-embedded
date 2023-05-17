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
package com.sbbsystems.statefun.tasks;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineFunctionProvider implements StatefulFunctionProvider {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineFunctionProvider.class);
    private final FunctionType callbackFunctionType;

    public PipelineFunctionProvider(FunctionType callbackFunctionType) {
        this.callbackFunctionType = callbackFunctionType;
    }

    public StatefulFunction functionOfType(FunctionType type) {
        LOG.info("Creating PipelineFunction instance");
        return new PipelineFunction(this.callbackFunctionType);
    }
}
