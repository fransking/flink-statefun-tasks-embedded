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
package com.sbbsystems.statefun.tasks.e2e;

import com.sbbsystems.statefun.tasks.PipelineFunctionModule;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

import java.util.Map;

public class EndToEndFunctionModule implements StatefulFunctionModule {
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        var mainModule = new PipelineFunctionModule();

        var configuration = PipelineConfiguration.of(
                "e2e/embedded_pipeline",
                "example/kafka-generic-egress",
                "example/kafka-generic-egress",
                "statefun-tasks.events",
                null);

        mainModule.configure(configuration, binder);
        binder.bindFunctionProvider(EndToEndRemoteFunction.FUNCTION_TYPE, unused -> new EndToEndRemoteFunction());
    }
}
