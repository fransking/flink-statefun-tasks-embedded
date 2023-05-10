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
package com.sbbsystems.statefun.tasks.graph;

import com.sbbsystems.statefun.tasks.util.TimedBlock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public final class PipelineGraphTests {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineGraphTests.class);

    private PipelineGraph createGraphFromTemplate(List<?> template) {
        var pipeline = PipelineGraphBuilderTests.buildPipelineFromTemplate(template);
        var builder = PipelineGraphBuilder.newInstance().fromProto(pipeline);
        return builder.build();
    }

//    @Test
//    public void graph_yields_all_tasks() {
//        var longList = new LinkedList<String>();
//        var template = new LinkedList<>();
//
//        try (var ignored = new TimedBlock(LOG, "Creating task template")) {
//            for (var i = 0; i < 1000000; i++) {
//                longList.add("cc" + i);
//            }
//
//
//            var group2 = List.of(
//                    List.of("aa"),
//                    List.of("bb"),
//                    longList
//            );
//
//            var group = List.of(
//                    List.of("a", "b", "c"),
//                    List.of("d", group2, "f")
//            );
//
//            template.add("1");
//            template.add(group);
//            template.add("2");
//        }
//
//        Graph graph;
//        try (var ignored = new TimedBlock(LOG, "Building the graph")) {
//            graph = this.createGraphFromTemplate(template);
//        }
//
//        int count = 0;
//        try (var ignored = new TimedBlock(LOG, "Enumerating the graph")) {
//            for (TaskId task : graph.getTasks()) {
//                graph.getTask(task.getId());
//                count++;
//            }
//        }
//
//        assertThat(count).isEqualTo(1000009);
//    }
//
//    @Test
//    public void get_next_step_in_chain_of_tasks() {
//        var template = List.of("1", "2", "3");
//        var graph = this.createGraphFromTemplate(template);
//
//        var firstStep = TaskId.of("1");
//        var nextStep = graph.getNextStep(firstStep);
//
//        //assertThat(nextStep.getId()).isEqualTo("2");
//    }
}
