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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.sdk.state.Expiration;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Objects;

public final class PipelineConfiguration {
    private static final String ID_FIELD = "id";
    private static final String STATE_EXPIRATION_FIELD = "stateExpiration";
    private static final String EGRESS_FIELD = "egress";
    private static final String EVENTS_EGRESS_FIELD = "eventsEgress";
    private static final String EVENTS_TOPIC_FIELD = "eventsTopic";

    private final String pipelineId;
    private final String eventsEgress;
    private final String eventsTopic;
    private final String stateExpiration;
    private final String egress;

    public static PipelineConfiguration fromNode(JsonNode jsonNode) {
        var specNode = (ObjectNode) jsonNode;

        var id = specNode.get(ID_FIELD);
        var egress = specNode.get(EGRESS_FIELD);
        var eventsEgress =  specNode.get(EVENTS_EGRESS_FIELD);
        var eventsTopic =  specNode.get(EVENTS_TOPIC_FIELD);
        var stateExpiration =  specNode.get(STATE_EXPIRATION_FIELD);

        return new PipelineConfiguration(
                id.asText(),
                egress.asText(),
                eventsEgress == null ? null : eventsEgress.asText(),
                eventsTopic == null ? null : eventsTopic.asText(),
                stateExpiration == null ? null : stateExpiration.asText()
        );
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
