/*
 * Copyright [2025] [Frans King]
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
package com.sbbsystems.statefun.tasks.graph.v2;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public final class FlinkSerializationTests {

    @Test
    public void types_are_pojo_serializable() {
        Map<Class<?>, String> serializableClasses = new HashMap<>();

        for (Class<?> cls : List.of(
                GraphEntry.class,
                MapOfGraphEntries.class
        )) {
            var config = new ExecutionConfig();
            TypeSerializer<?> serializer = TypeExtractor.getForClass(cls).createSerializer(config);

            serializableClasses.put(cls, serializer.getClass().getName());
        }

        for (Map.Entry<Class<?>, String> entry : serializableClasses.entrySet()) {
            assertThat(entry.getValue()).contains("PojoSerializer");
        }
    }

    @Test
    public void task_entry_is_pojo_serialized() throws IOException {
        var config = new ExecutionConfig();
        config.disableGenericTypes();
        TypeSerializer<GraphEntry> typeSerializer = TypeExtractor.getForClass(GraphEntry.class).createSerializer(config);

        GraphEntry task = new GraphEntry();
        task.setNextId(new GraphEntry().getId());

        DataOutputSerializer serializer = new DataOutputSerializer(100);
        typeSerializer.serialize(task, serializer);
    }

    @Test
    public void group_entry_is_pojo_serialized() throws IOException {
        var config = new ExecutionConfig();
        config.disableGenericTypes();
        TypeSerializer<GraphEntry> typeSerializer = TypeExtractor.getForClass(GraphEntry.class).createSerializer(config);

        GraphEntry task = new GraphEntry();
        task.setNextId(new GraphEntry().getId());
        task.setIdsInGroup(List.of(new GraphEntry().getId()));

        DataOutputSerializer serializer = new DataOutputSerializer(100);
        typeSerializer.serialize(task, serializer);
    }

    @Test
    public void map_of_entries_is_pojo_serialized() throws IOException {
        var config = new ExecutionConfig();
        config.disableGenericTypes();
        TypeSerializer<MapOfGraphEntries> typeSerializer = TypeExtractor.getForClass(MapOfGraphEntries.class).createSerializer(config);

        MapOfGraphEntries map = new MapOfGraphEntries();
        map.setItems(Map.of("id",  new GraphEntry()));

        DataOutputSerializer serializer = new DataOutputSerializer(100);
        typeSerializer.serialize(map, serializer);
    }
}
