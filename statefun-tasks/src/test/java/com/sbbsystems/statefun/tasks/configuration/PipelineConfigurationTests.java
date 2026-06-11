/*
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
package com.sbbsystems.statefun.tasks.configuration;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PipelineConfigurationTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static ObjectNode baseNode() {
        var node = MAPPER.createObjectNode();
        node.put("id", "example/pipeline");
        node.put("egress", "example/egress");
        return node;
    }

    // ---- of() factories ------------------------------------------------------------------------

    @Test
    public void of_short_form_defaults_use_legacy_types_to_false() {
        var config = PipelineConfiguration.of("example/pipeline", "example/egress");
        assertThat(config.isUseLegacyTypes()).isFalse();
    }

    @Test
    public void of_long_form_defaults_use_legacy_types_to_false() {
        var config = PipelineConfiguration.of(
                "example/pipeline", "example/egress",
                null, null, null, 10);
        assertThat(config.isUseLegacyTypes()).isFalse();
    }

    // ---- fromNode() ----------------------------------------------------------------------------

    @Test
    public void fromNode_defaults_use_legacy_types_to_false_when_absent() {
        var config = PipelineConfiguration.fromNode(baseNode());
        assertThat(config.isUseLegacyTypes()).isFalse();
    }

    @Test
    public void fromNode_sets_use_legacy_types_to_false_when_explicitly_false() {
        var node = baseNode();
        node.put("useLegacyTypes", false);
        var config = PipelineConfiguration.fromNode(node);
        assertThat(config.isUseLegacyTypes()).isFalse();
    }

    @Test
    public void fromNode_sets_use_legacy_types_to_true_when_true() {
        var node = baseNode();
        node.put("useLegacyTypes", true);
        var config = PipelineConfiguration.fromNode(node);
        assertThat(config.isUseLegacyTypes()).isTrue();
    }

    // ---- other fromNode fields -----------------------------------------------------------------

    @Test
    public void fromNode_parses_namespace_and_type_from_id() {
        var config = PipelineConfiguration.fromNode(baseNode());
        assertThat(config.getNamespace()).isEqualTo("example");
        assertThat(config.getType()).isEqualTo("pipeline");
    }

    @Test
    public void fromNode_parses_callback_delay_ms() {
        var node = baseNode();
        node.put("callbackDelayMs", 50);
        var config = PipelineConfiguration.fromNode(node);
        assertThat(config.getCallbackDelay().toMillis()).isEqualTo(50);
    }

    @Test
    public void fromNode_defaults_callback_delay_ms_when_absent() {
        var config = PipelineConfiguration.fromNode(baseNode());
        assertThat(config.getCallbackDelay().toMillis()).isEqualTo(10);
    }

    @Test
    public void fromNode_parses_events_egress_and_topic() {
        var node = baseNode();
        node.put("eventsEgress", "example/events-egress");
        node.put("eventsTopic", "my-topic");
        var config = PipelineConfiguration.fromNode(node);
        assertThat(config.hasEventsEgress()).isTrue();
        assertThat(config.getEventsEgressNamespace()).isEqualTo("example");
        assertThat(config.getEventsEgressType()).isEqualTo("events-egress");
        assertThat(config.getEventsTopic()).isEqualTo("my-topic");
    }

    @Test
    public void fromNode_has_no_events_egress_when_absent() {
        var config = PipelineConfiguration.fromNode(baseNode());
        assertThat(config.hasEventsEgress()).isFalse();
    }
}

