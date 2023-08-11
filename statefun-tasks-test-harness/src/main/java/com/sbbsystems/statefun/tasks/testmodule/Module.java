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
package com.sbbsystems.statefun.tasks.testmodule;

import com.sbbsystems.statefun.tasks.configuration.PipelineBinderV1;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

import java.text.MessageFormat;
import java.util.Map;

public class Module implements StatefulFunctionModule {
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        var configuration = PipelineConfiguration.of(
                "example/embedded_pipeline",
                "example/kafka-generic-egress",
                MessageFormat.format("{0}/{1}", IoIdentifiers.EVENTS_EGRESS.namespace(), IoIdentifiers.EVENTS_EGRESS.name()),
                IoIdentifiers.EVENTS_TOPIC,
                null);

        PipelineBinderV1.INSTANCE.bind(configuration, binder);
        binder.bindFunctionProvider(IoIdentifiers.ECHO_FUNCTION_TYPE, unused -> new EchoFunction());
        binder.bindIngressRouter(IoIdentifiers.REQUEST_INGRESS, new TestPipelineRouter());
    }
}
