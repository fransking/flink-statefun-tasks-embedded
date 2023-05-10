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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public final class PipelineGraphTests {

    private PipelineGraph fromTemplate(List<?> template) {
        var pipeline = PipelineGraphBuilderTests.buildPipelineFromTemplate(template);
        var builder = PipelineGraphBuilder.newInstance().fromProto(pipeline);
        return builder.build();
    }

    @Test
    public void can_fetch_task_given_id() {
        var template = List.of("a", "b", "c");
        var graph = fromTemplate(template);

        var a = graph.getTask("a");
        var b = graph.getTask("b");
        var c = graph.getTask("c");

        assertThat(a).isNotNull();
        assertThat(b).isEqualTo(a.getNext());
        assertThat(c).isEqualTo(b.getNext());
    }

    @Test
    public void basic_graph_structure_is_correct() {
        var nestedGroup = List.of(
                List.of("x", "y", "z")
        );

        var group = List.of(
                List.of("a", nestedGroup, "b")
        );

        var template = List.of("one", group, "two");
        var graph = fromTemplate(template);

        var one = graph.getTask("one");
        var a = graph.getTask("a");
        var b = graph.getTask("b");
        var x = graph.getTask("x");
        var y = graph.getTask("y");
        var z = graph.getTask("z");
        var two = graph.getTask("two");

        //parent group hierarchy
        assertThat(one.getParentGroup()).isNull();
        assertThat(two.getParentGroup()).isNull();
        assertThat(a.getParentGroup()).isNotNull();
        assertThat(b.getParentGroup()).isEqualTo(a.getParentGroup());
        assertThat(b.getParentGroup()).isNotNull();
        assertThat(b.getParentGroup().getParentGroup()).isNull();
        assertThat(x.getParentGroup()).isNotNull();
        assertThat(y.getParentGroup()).isEqualTo(x.getParentGroup());
        assertThat(z.getParentGroup()).isEqualTo(x.getParentGroup());
        assertThat(x.getParentGroup().getParentGroup()).isEqualTo(a.getParentGroup());

        //task chain
        assertThat(one.getNext()).isEqualTo(a.getParentGroup());
        assertThat(a.getNext()).isEqualTo(x.getParentGroup());
        assertThat(a.getParentGroup().getNext()).isEqualTo(two);
        assertThat(x.getParentGroup().getNext()).isEqualTo(b);
        assertThat(x.getNext()).isEqualTo(y);
        assertThat(b.getNext()).isNull();
        assertThat(z.getNext()).isNull();
        assertThat(two.getNext()).isNull();
    }
}
