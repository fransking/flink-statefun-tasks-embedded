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

import com.sbbsystems.statefun.tasks.batchcallback.CallbackFunctionProvider;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfigurationLoader;
import org.apache.flink.statefun.flink.common.ResourceLocator;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.text.MessageFormat;
import java.util.Map;

public class PipelineFunctionModule implements StatefulFunctionModule {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineFunctionModule.class);

    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        LOG.info("Embedded pipeline function module loaded");

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        var config = StatefulFunctionsConfig.fromEnvironment(env);

        for (var pipelineConfiguration: PipelineConfigurationLoader.loadFrom(config)) {
            configure(pipelineConfiguration, binder);
        }
    }

    public void configure(PipelineConfiguration configuration, Binder binder) {
        try {
            var pipelineFunctionType = new FunctionType(configuration.getNamespace(), configuration.getType());
            var callbackFunctionType = new FunctionType(configuration.getNamespace(), configuration.getCallbackType());

            binder.bindFunctionProvider(pipelineFunctionType, PipelineFunctionProvider.of(configuration, callbackFunctionType));
            binder.bindFunctionProvider(callbackFunctionType, CallbackFunctionProvider.of(pipelineFunctionType));

            LOG.info("Embedded pipeline function registered");
        } catch (IllegalStateException e) {
            LOG.error("Error registering embedded pipeline function", e);
        }
    }
}
