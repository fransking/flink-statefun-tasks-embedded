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

import com.google.common.base.Strings;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonPointer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.statefun.flink.common.json.Selectors;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class PipelineConfiguration {
    private static final JsonPointer PIPELINES = JsonPointer.compile("/module/spec/pipelines");
    private static final JsonPointer PIPELINE_META_ID = JsonPointer.compile("/pipeline/meta/id");
    private static final JsonPointer PIPELINE_SPEC_STATE_EXPIRATION = JsonPointer.compile("/pipeline/spec/stateExpiration");
    private static final JsonPointer PIPELINE_SPEC_EGRESS = JsonPointer.compile("/pipeline/spec/egress");
    private static final JsonPointer PIPELINE_SPEC_EVENTS_EGRESS = JsonPointer.compile("/pipeline/spec/eventsEgress");
    private static final JsonPointer PIPELINE_SPEC_EVENTS_TOPIC = JsonPointer.compile("/pipeline/spec/eventsTopic");

    private final String pipelineId;
    private final String eventsEgress;
    private final String eventsTopic;
    private final String stateExpiration;
    private final String egress;

    public static Stream<PipelineConfiguration> fromModuleYaml(JsonNode node) {
        var pipelineNodes = Selectors.listAt(node, PIPELINES);

        return StreamSupport.stream(pipelineNodes.spliterator(), false).map(PipelineConfiguration::from);
    }

    public static PipelineConfiguration of(
            @NotNull String pipelineId,
            @NotNull String egress,
            String eventsEgress,
            String eventsTopic,
            String stateExpiration) {

        return new PipelineConfiguration(
                Objects.requireNonNull(pipelineId),
                Objects.requireNonNull(egress),
                eventsEgress,
                eventsTopic,
                stateExpiration
        );
    }

    public static PipelineConfiguration of(@NotNull String pipelineId, @NotNull String egress) {
        return new PipelineConfiguration(
                Objects.requireNonNull(pipelineId),
                Objects.requireNonNull(egress),
                null,
                null,
                null
        );
    }

    private static PipelineConfiguration from(JsonNode pipelineNode) {
        return new PipelineConfiguration(
                Selectors.textAt(pipelineNode, PIPELINE_META_ID),
                Selectors.textAt(pipelineNode, PIPELINE_SPEC_EGRESS),
                Selectors.optionalTextAt(pipelineNode, PIPELINE_SPEC_EVENTS_EGRESS).orElse(null),
                Selectors.optionalTextAt(pipelineNode, PIPELINE_SPEC_EVENTS_TOPIC).orElse(null),
                Selectors.optionalTextAt(pipelineNode, PIPELINE_SPEC_STATE_EXPIRATION).orElse(null)
        );
    }

    private PipelineConfiguration(
            String pipelineId,
            String egress,
            String eventsEgress,
            String eventsTopic,
            String stateExpiration) {
        this.pipelineId = pipelineId;
        this.egress = egress;
        this.eventsEgress = eventsEgress;
        this.eventsTopic = eventsTopic;
        this.stateExpiration = stateExpiration;
    }

    public String getNamespace() {
        return pipelineId.split("/")[0];
    }

    public String getType() {
        return pipelineId.split("/")[1];
    }

    public String getCallbackType() {
        return pipelineId.split("/")[1] + "_callback";
    }

    public Expiration getStateExpiration() {
        if (Strings.isNullOrEmpty(stateExpiration)) {
            return Expiration.none();
        }

        return Expiration.expireAfter(Duration.parse(stateExpiration), Expiration.Mode.AFTER_READ_OR_WRITE);
    }

    public String getEgressNamespace() {
        return egress.split("/")[0];
    }

    public String getEgressType() {
        return egress.split("/")[1];
    }

    public String getEventsEgressNamespace() {
        return eventsEgress.split("/")[0];
    }
    public String getEventsEgressType() {
        return eventsEgress.split("/")[1];
    }

    public String getEventsTopic() {
        return eventsTopic;
    }

    public boolean hasEventsEgress() {
        return !Strings.isNullOrEmpty(eventsEgress) && eventsEgress.contains("/") && !Strings.isNullOrEmpty(eventsTopic);
    }
}
