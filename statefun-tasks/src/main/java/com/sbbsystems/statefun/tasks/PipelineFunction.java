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
package com.sbbsystems.statefun.tasks;

import com.sbbsystems.statefun.tasks.messagehandlers.MessageHandler;
import com.sbbsystems.statefun.tasks.messagehandlers.TaskRequestHandler;
import com.sbbsystems.statefun.tasks.util.TimedBlock;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.List;

public class PipelineFunction implements StatefulFunction {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineFunction.class);

    private static final List<MessageHandler<?>> messageHandlers = List.of(
            new TaskRequestHandler()
    );

    @Override
    public void invoke(Context context, Object input) {
        var logMessage = MessageFormat.format("Invoking function {0}", context.self());

        try (var ignored = TimedBlock.of(LOG::info, logMessage)) {
            for (var handler : messageHandlers) {
                if (handler.canHandle(context, input)) {
                    handler.handleInput(context, input);
                    break;
                }
            }
        }
    }
}
