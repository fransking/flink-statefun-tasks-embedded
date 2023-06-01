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
import com.sbbsystems.statefun.tasks.util.Unchecked;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GroupResultAggregator {
    private GroupResultAggregator() {
    }

    public static GroupResultAggregator newInstance() {
        return new GroupResultAggregator();
    }

    public TaskResultOrException aggregateResult(String groupId, String invocationId,
                                                 Iterable<TaskResultOrException> orderedGroupResults, boolean returnExceptions) {
        var aggregatedState = aggregateState(orderedGroupResults);

        if (returnExceptions) {
            return wrapResultsList(groupId, orderedGroupResults, aggregatedState);
        } else {
            var hasException = false;
            for (var resultOrException : orderedGroupResults) {
                if (resultOrException.hasTaskException()) {
                    hasException = true;
                    break;
                }
            }
            if (hasException) {
                return buildTaskException(groupId, invocationId, orderedGroupResults, aggregatedState);
            } else {
                return wrapResultsList(groupId, orderedGroupResults, aggregatedState);
            }
        }
    }

    private static TaskResultOrException buildTaskException(String groupId, String invocationId, Iterable<TaskResultOrException> orderedGroupResults,
                                                            Any aggregatedState) {
        var exceptionMessage = new StringBuilder();
        var stackTrace = new StringBuilder();
        for (var resultOrException : orderedGroupResults) {
            if (resultOrException.hasTaskResult()) {
                continue;
            }
            if (exceptionMessage.length() > 0) {
                exceptionMessage.append('|');
                stackTrace.append('|');
            }
            var taskException = resultOrException.getTaskException();
            exceptionMessage.append(taskException.getId())
                    .append(", ")
                    .append(taskException.getExceptionType())
                    .append(", ")
                    .append(taskException.getExceptionMessage());

            stackTrace.append(taskException.getId())
                    .append(", ")
                    .append(taskException.getStacktrace());
        }
        var taskException = TaskException.newBuilder()
                .setId(groupId)
                .setExceptionType("__aggregate.error")
                .setExceptionMessage(exceptionMessage.toString())
                .setStacktrace(stackTrace.toString())
                .setState(aggregatedState)
                .setInvocationId(invocationId)
                .build();
        return TaskResultOrException.newBuilder()
                .setTaskException(taskException)
                .build();
    }

    private TaskResultOrException wrapResultsList(String groupId, Iterable<TaskResultOrException> orderedGroupResults, Any aggregatedState) {
        var tupleResultBuilder = TupleOfAny.newBuilder();
        for (var resultOrException : orderedGroupResults) {
            if (resultOrException.hasTaskException()) {
                tupleResultBuilder.addItems(Any.pack(resultOrException.getTaskException()));
            } else {
                tupleResultBuilder.addItems(resultOrException.getTaskResult().getResult());
            }
        }
        var taskResult = TaskResult.newBuilder()
                .setUid(groupId)
                .setResult(Any.pack(tupleResultBuilder.build()))
                .setState(aggregatedState)
                .build();
        return TaskResultOrException.newBuilder()
                .setTaskResult(taskResult)
                .build();
    }

    private Any aggregateState(Iterable<TaskResultOrException> results) {
        if (!results.iterator().hasNext()) {
            return Any.pack(NoneValue.getDefaultInstance());
        }
        var resultStates = StreamSupport.stream(results.spliterator(), false)
                .map(item -> item.hasTaskResult() ? item.getTaskResult().getState() : item.getTaskException().getState())
                .collect(Collectors.toUnmodifiableList());

        // state aggregation is only supported when each task returns state as a MapOfStringToAny
        var canAggregate = resultStates.stream()
                .allMatch(state -> state.is(MapOfStringToAny.class));

        if (canAggregate) {
            var mergedMap = MapOfStringToAny.newBuilder();
            resultStates.forEach(Unchecked.<Any, InvalidProtocolBufferException>unchecked(
                    itemState -> mergedMap.putAllItems(itemState.unpack(MapOfStringToAny.class).getItemsMap())));
            return Any.pack(mergedMap.build());
        } else {
            return resultStates.get(0);
        }
    }
}
