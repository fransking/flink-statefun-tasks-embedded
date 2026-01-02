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

import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.Pipeline;
import com.sbbsystems.statefun.tasks.generated.TaskResult;
import com.sbbsystems.statefun.tasks.utils.NamespacedTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.sbbsystems.statefun.tasks.e2e.MoreStrings.asString;
import static com.sbbsystems.statefun.tasks.e2e.PipelineBuilder.inParallel;
import static org.assertj.core.api.Assertions.assertThat;

public class MaxParallelismTests {
    private NamespacedTestHarness harness;

    @BeforeEach
    void setup() {
        harness = NamespacedTestHarness.newInstance();
    }

    @Test
    public void test_max_parallelism() throws InvalidProtocolBufferException {
        var pipeline = createParallelPipeline(20, 0);

        var response = harness.runPipelineAndGetResponse(pipeline);
        var result = asString(response.unpack(TaskResult.class).getResult());

        var maxConcurrentTasks = findMaxConcurrentTasks(result);

        assertThat(maxConcurrentTasks).isGreaterThan(1);
    }

    @Test
    public void test_max_parallelism_one() throws InvalidProtocolBufferException {
        var pipeline = createParallelPipeline(10, 1);

        var response = harness.runPipelineAndGetResponse(pipeline);
        var result = asString(response.unpack(TaskResult.class).getResult());

        var maxConcurrentTasks = findMaxConcurrentTasks(result);

        assertThat(maxConcurrentTasks).isEqualTo(1);
    }

    @Test
    public void test_max_parallelism_one_on_continuation() throws InvalidProtocolBufferException {
        var pipeline = PipelineBuilder.beginWith("echo", Int32Value.of(100))
                .continueWith(createParallelPipeline(2, 1))
                .build();

        var response = harness.runPipelineAndGetResponse(pipeline);
        var result = asString(response.unpack(TaskResult.class).getResult());

        var maxConcurrentTasks = findMaxConcurrentTasks(result);

        assertThat(maxConcurrentTasks).isEqualTo(1);
    }

    private static Pipeline createParallelPipeline(int nTasks, int maxParallelism) {
        var tasks = IntStream.range(0, nTasks)
                .boxed()
                .map(i -> PipelineBuilder
                        .beginWith("delay")
                        .build())
                .collect(Collectors.toList());

        return inParallel(tasks, maxParallelism).build();
    }

    private static long findMaxConcurrentTasks(String result) {
        var startAndEndTimes = new ArrayList<TaskRunTimes>();
        String[] taskResults = result.substring(1, result.length() - 1).split(",");
        for (var taskResult : taskResults) {
            var splitResult = taskResult.trim().split("\\|");
            var startTime = LocalDateTime.parse(splitResult[0]);
            var endTime = LocalDateTime.parse(splitResult[1]);
            startAndEndTimes.add(TaskRunTimes.of(startTime, endTime));
        }

        return startAndEndTimes.stream().mapToLong(
                itemTimes ->
                        startAndEndTimes.stream().filter(otherTaskTimes ->
                                (!itemTimes.getStart().isBefore(otherTaskTimes.getStart()) && !itemTimes.getStart().isAfter(otherTaskTimes.getEnd()))
                                        || (!itemTimes.getEnd().isBefore(otherTaskTimes.getStart()) && !itemTimes.getEnd().isAfter(otherTaskTimes.getEnd()))
                        ).count()
        ).max().getAsLong();
    }

    private static class TaskRunTimes {
        private final LocalDateTime start;
        private final LocalDateTime end;

        private TaskRunTimes(LocalDateTime start, LocalDateTime end) {
            this.start = start;
            this.end = end;
        }

        public static TaskRunTimes of(LocalDateTime startTime, LocalDateTime endTime) {
            return new TaskRunTimes(startTime, endTime);
        }

        public LocalDateTime getStart() {
            return start;
        }

        public LocalDateTime getEnd() {
            return end;
        }

    }
}
