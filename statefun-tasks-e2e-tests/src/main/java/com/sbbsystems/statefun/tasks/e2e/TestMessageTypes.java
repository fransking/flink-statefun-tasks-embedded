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

import com.google.protobuf.Message;
import com.sbbsystems.statefun.tasks.generated.ArgsAndKwargs;
import com.sbbsystems.statefun.tasks.generated.MapOfStringToAny;

import java.util.Map;

import static com.sbbsystems.statefun.tasks.types.MessageTypes.packAny;

public class TestMessageTypes {
    public static ArgsAndKwargs toArgsAndKwargs(Map<String, Message> keywordArgs) {
        var kwargs = MapOfStringToAny.newBuilder();
        keywordArgs.forEach((k, v) -> kwargs.putItems(k, packAny(v)));
        return ArgsAndKwargs.newBuilder().setKwargs(kwargs.build()).build();
    }
}
