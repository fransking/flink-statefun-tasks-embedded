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

package com.sbbsystems.statefun.tasks.batchcallback.messagehandlers;

import com.sbbsystems.statefun.tasks.batchcallback.CallbackFunctionState;
import com.sbbsystems.statefun.tasks.generated.ResultsBatch;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SimpleBatchSubmitter implements BatchSubmitter {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleBatchSubmitter.class);
    private final FunctionType pipelineFunctionType;

    private SimpleBatchSubmitter(FunctionType pipelineFunctionType) {
        this.pipelineFunctionType = pipelineFunctionType;
    }

    public static SimpleBatchSubmitter of(FunctionType pipelineFunctionType) {
        return new SimpleBatchSubmitter(pipelineFunctionType);
    }

    @Override
    public void trySubmitBatch(Context context, CallbackFunctionState state) {
        if (state.getPipelineRequestInProgress().getOrDefault(false)) {
            return;
        }
        var batchList = StreamSupport.stream(state.getBatch().view().spliterator(), false)
                .collect(Collectors.toList());
        if (batchList.size() > 0) {
            var functionId = context.self().id();
            LOG.info("{} - Sending batch of {} results", functionId, batchList.size());
            var batchMessage = ResultsBatch.newBuilder()
                    .addAllResults(batchList)
                    .build();
            var message = MessageTypes.wrap(batchMessage);
            context.send(this.pipelineFunctionType, functionId, message);
            state.getPipelineRequestInProgress().set(true);
            state.getBatch().clear();
        }
    }
}
