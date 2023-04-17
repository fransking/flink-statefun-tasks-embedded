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
package com.sbbsystems.flink.tasks.embedded;

import java.util.Map;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineFunctionModule implements StatefulFunctionModule {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineFunctionModule.class);

    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        // Embedded functions, ingresses, routers, and egresses
        // can be bound to the Binder

        LOG.info("Flink Statefun Tasks embedded pipeline Function module loaded");
    }
}

