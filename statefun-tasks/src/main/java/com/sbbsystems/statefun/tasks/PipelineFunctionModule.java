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
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PipelineFunctionModule implements StatefulFunctionModule {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineFunctionModule.class);

    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        LOG.info("Embedded pipeline function module loaded");

        //todo make this configurable from the module.yaml
        try {
            var pipelineFunctionType = new FunctionType("example", "embedded_pipeline");
            var callbackFunctionType = new FunctionType("example", "embedded_pipeline_callback");
            var pipelineProvider = new PipelineFunctionProvider(callbackFunctionType);
            var callbackProvider = new CallbackFunctionProvider(pipelineFunctionType);

            binder.bindFunctionProvider(pipelineFunctionType, pipelineProvider);
            binder.bindFunctionProvider(callbackFunctionType, callbackProvider);

            LOG.info("Embedded pipeline function registered");
        } catch (IllegalStateException e) {
            LOG.warn("Error registering pipeline function", e);
        }
    }
}
