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
package com.sbbsystems.statefun.tasks.configuration;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.statefun.flink.common.ResourceLocator;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;

import java.io.IOException;
import java.net.URL;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class PipelineConfigurationLoader {
    public static Iterable<PipelineConfiguration> loadFrom(StatefulFunctionsConfig config) {
        Iterable<URL> namedResources = ResourceLocator.findResources(config.getRemoteModuleName());

        return StreamSupport.stream(namedResources.spliterator(), false)
                .flatMap(PipelineConfigurationLoader::loadFromUrl)
                .collect(Collectors.toList());
    }

    private static Stream<PipelineConfiguration> loadFromUrl(URL moduleUrl) {
        try (var parser = new YAMLFactory().createParser(moduleUrl)) {
            var jsonNodes = new ObjectMapper().readValues(parser, JsonNode.class);
            return jsonNodes.readAll().stream().flatMap(PipelineConfiguration::fromModuleYaml);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private PipelineConfigurationLoader() {}
}
