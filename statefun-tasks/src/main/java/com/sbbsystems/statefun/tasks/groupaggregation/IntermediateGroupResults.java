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

import com.sbbsystems.statefun.tasks.PipelineFunctionState;
import com.sbbsystems.statefun.tasks.generated.TaskResultOrException;
import com.sbbsystems.statefun.tasks.graph.Entry;
import com.sbbsystems.statefun.tasks.graph.Group;
import org.apache.flink.statefun.sdk.state.*;

import java.util.Collections;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.isNull;

public class IntermediateGroupResults {
    private final PipelineFunctionState state;

    public static IntermediateGroupResults from(PipelineFunctionState state) {
        return new IntermediateGroupResults(state);
    }
    
    private IntermediateGroupResults(PipelineFunctionState state) {
        this.state = state;
    }

    public void addResult(Group group, Entry entry, TaskResultOrException resultOrException) {
        if (group.isUnordered()) {
            // unordered groups are quicker to aggregate as they use lists instead of maps
            addUnOrderedIntermediateGroupResult(group.getId(), resultOrException);
        } else {
            state.getIntermediateGroupResults().set(entry.getChainHead().getId(), resultOrException);
        }
    }

    public Stream<TaskResultOrException> getResults(Group group) {
        if (group.isUnordered()) {
            var unorderedResults = getUnorderedResults(group.getId());
            clearUnorderedResults(group.getId());
            return StreamSupport.stream(unorderedResults.spliterator(), false);
        } else {
            return group
                    .getItems()
                    .stream()
                    .map(entry -> state.getIntermediateGroupResults().get(entry.getChainHead().getId()));
        }
    }

    private void addUnOrderedIntermediateGroupResult(String groupId, TaskResultOrException resultOrException) {
        var groupResults = state.getUnOrderedIntermediateGroupResults().get(groupId);

        if (isNull(groupResults)) {
            groupResults = PersistedAppendingBuffer.of(groupId, TaskResultOrException.class, state.getExpiration());
            state.getDynamicStateRegistry().registerAppendingBuffer(groupResults);
            state.getUnOrderedIntermediateGroupResults().put(groupId, groupResults);
        }

        groupResults.append(resultOrException);
    }

    private Iterable<TaskResultOrException> getUnorderedResults(String groupId) {
        var groupResults = state.getUnOrderedIntermediateGroupResults().get(groupId);
        return (isNull(groupResults)) ? Collections::emptyIterator : groupResults.view();
    }

    private void clearUnorderedResults(String groupId) {
        state.getUnOrderedIntermediateGroupResults().remove(groupId);
    }
}
