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

package com.sbbsystems.statefun.tasks.groupaggregation;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.*;

import java.util.LinkedList;
import java.util.stream.Stream;

import static com.sbbsystems.statefun.tasks.util.Unchecked.unchecked;


public class GroupResultAggregator {

    public static GroupResultAggregator newInstance( ) {
        return new GroupResultAggregator();
    }

    private GroupResultAggregator() {
    }

    public TaskResultOrException aggregateResults(String groupId,
                                                  String invocationId,
                                                  Stream<TaskResultOrException> orderedGroupResults,
                                                  boolean hasException,
                                                  boolean returnExceptions) {

        var unaggregatedState = new LinkedList<Any>();
        var aggregatedState = MapOfStringToAny.newBuilder();

        var aggregatedResults = TupleOfAny.newBuilder();
        var aggregatedExceptionMessages = new StringBuilder();
        var aggregatedExceptionStackTraces = new StringBuilder();

        orderedGroupResults
                .map(result -> getResult(result, returnExceptions))
                .forEach(unchecked(result -> {

            if (result.hasTaskResult()) {
                var taskResult = result.getTaskResult();

                aggregatedResults.addItems(taskResult.getResult());
                aggregateState(taskResult.getState(), unaggregatedState, aggregatedState);

            } else if (result.hasTaskException()) {
                var taskException = result.getTaskException();

                if (aggregatedExceptionMessages.length() > 0) {
                    aggregatedExceptionMessages.append('|');
                    aggregatedExceptionStackTraces.append('|');
                }
                aggregatedExceptionMessages.append(taskException.getId())
                        .append(", ")
                        .append(taskException.getExceptionType())
                        .append(", ")
                        .append(taskException.getExceptionMessage());

                aggregatedExceptionStackTraces.append(taskException.getId())
                        .append(", ")
                        .append(taskException.getStacktrace());

                aggregateState(taskException.getState(), unaggregatedState, aggregatedState);
            }

        }));

        var state = (unaggregatedState.isEmpty())
                ? Any.pack(aggregatedState.build())
                : unaggregatedState.getFirst();

        if (hasException && !returnExceptions) {
            var taskException = TaskException.newBuilder()
                    .setId(groupId)
                    .setUid(groupId)
                    .setInvocationId(invocationId)
                    .setType("__aggregate.error")
                    .setExceptionType("statefun_tasks.AggregatedError")
                    .setExceptionMessage(aggregatedExceptionMessages.toString())
                    .setStacktrace(aggregatedExceptionStackTraces.toString())
                    .setState(state)
                    .build();
            return TaskResultOrException.newBuilder()
                    .setTaskException(taskException)
                    .build();
        } else {
            var taskResult = TaskResult.newBuilder()
                    .setId(groupId)
                    .setUid(groupId)
                    .setInvocationId(invocationId)
                    .setType("__aggregate.result")
                    .setResult(Any.pack(aggregatedResults.build()))
                    .setState(state)
                    .build();
            return TaskResultOrException.newBuilder()
                    .setTaskResult(taskResult)
                    .build();
        }
    }

    private void aggregateState(Any state, LinkedList<Any> unaggregatedState, MapOfStringToAny.Builder aggregatedState)
            throws InvalidProtocolBufferException {

        if (unaggregatedState.isEmpty()) {
            if (state.is(MapOfStringToAny.class)) {
                mergeItems(state.unpack(MapOfStringToAny.class), aggregatedState);
            } else {
                unaggregatedState.add(state); // we will only ever add one item
            }
        }
    }

    private void mergeItems(MapOfStringToAny from, MapOfStringToAny.Builder into) {
        from.getItemsMap().forEach((key ,value) -> {
            if (!into.containsItems(key)) {
                into.putItems(key, value);
            }
        });
    }

    private static TaskResultOrException getResult(TaskResultOrException result, boolean returnExceptions) {
        if (result.hasTaskResult() || !returnExceptions) {
            return result;
        }

        // convert the TaskException to a TaskResult containing the TaskException
        var taskException = result.getTaskException();

        var taskResult = TaskResult.newBuilder()
                .setResult(Any.pack(taskException))
                .setState(taskException.getState())
                .build();
        return TaskResultOrException.newBuilder()
                .setTaskResult(taskResult)
                .build();
    }
}
