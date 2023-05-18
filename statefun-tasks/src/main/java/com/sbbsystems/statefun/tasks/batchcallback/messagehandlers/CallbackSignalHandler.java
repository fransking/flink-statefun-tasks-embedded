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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.batchcallback.CallbackFunctionState;
import com.sbbsystems.statefun.tasks.core.StatefunTasksException;
import com.sbbsystems.statefun.tasks.generated.CallbackSignal;
import com.sbbsystems.statefun.tasks.messagehandlers.MessageHandler;
import com.sbbsystems.statefun.tasks.types.InvalidMessageTypeException;
import com.sbbsystems.statefun.tasks.types.MessageTypes;
import com.sbbsystems.statefun.tasks.util.CheckedFunction;
import org.apache.flink.statefun.sdk.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;

public class CallbackSignalHandler extends MessageHandler<CallbackSignal, CallbackFunctionState> {
    private static final Logger LOG = LoggerFactory.getLogger(CallbackSignalHandler.class);
    private final BatchSubmitter batchSubmitter;

    private CallbackSignalHandler(BatchSubmitter batchSubmitter) {
        this.batchSubmitter = batchSubmitter;
    }

    public static CallbackSignalHandler newInstance(BatchSubmitter batchSubmitter) {
        return new CallbackSignalHandler(batchSubmitter);
    }


    @Override
    public boolean canHandle(Context context, Object input, CallbackFunctionState callbackFunctionState) {
        return MessageTypes.isType(input, CallbackSignal.class);
    }

    @Override
    public CheckedFunction<ByteString, CallbackSignal, InvalidProtocolBufferException> getMessageBuilder() {
        return CallbackSignal::parseFrom;
    }

    @Override
    public void handleMessage(Context context, CallbackSignal callbackSignal, CallbackFunctionState state)
            throws StatefunTasksException {

        var signal = callbackSignal.getValue();
        switch (signal) {
            case PIPELINE_STARTING:
                LOG.info("{} - starting pipeline", context.self().id());
                state.getBatch().clear();
                state.getPipelineRequestInProgress().set(false);
                break;
            case BATCH_PROCESSED:
                LOG.debug("{} - batch processed", context.self().id());
                state.getPipelineRequestInProgress().set(false);
                this.batchSubmitter.trySubmitBatch(context, state);
                break;
            case PAUSE_PIPELINE:
                state.getPaused().set(true);
                break;
            case RESUME_PIPELINE:
                state.getPaused().set(false);
                this.batchSubmitter.trySubmitBatch(context, state);
                break;
            default:
                throw new InvalidMessageTypeException(
                        MessageFormat.format("Unexpected CallbackSignal {0}", signal.getValueDescriptor().getName()));
        }
    }
}
