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

import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.generated.ArrayOfAny;
import com.sbbsystems.statefun.tasks.generated.MapOfStringToAny;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;

import java.util.List;
import java.util.Map;

public class MoreStrings {

    public static String asString(Any value)
            throws InvalidProtocolBufferException {

        return buildString(value).toString();
    }

    private static StringBuilder buildString(Any value)
            throws InvalidProtocolBufferException {

        var builder = new StringBuilder();

        if (value.is(TupleOfAny.class)) {
            builder.append("(");
            appendTo(builder, value.unpack(TupleOfAny.class).getItemsList());
            builder.append(")");

        } if (value.is(ArrayOfAny.class)) {
            builder.append("[");
            appendTo(builder, value.unpack(ArrayOfAny.class).getItemsList());
            builder.append("]");
        }

        else if (value.is(MapOfStringToAny.class)) {
            var map = value.unpack(MapOfStringToAny.class).getItemsMap();
            builder.append("{");
            appendTo(builder, map);
            builder.append("}");

        } else if (value.is(Int32Value.class)) {
            builder.append(value.unpack(Int32Value.class).getValue());
        }

        else if (value.is(StringValue.class)) {
            builder.append(value.unpack(StringValue.class).getValue());
        }

        return builder;
    }

    private static void appendTo(StringBuilder builder, Map<String, Any> map) throws InvalidProtocolBufferException {
        var i = 0;
        for (var key: map.keySet()) {
            builder.append(key);
            builder.append(": ");
            builder.append(buildString(map.get(key)));

            if (++i < map.size()) {
                builder.append(", ");
            }
        }
    }

    private static void appendTo(StringBuilder builder, List<Any> items) throws InvalidProtocolBufferException {
        var i = 0;
        for (var item: items) {
            builder.append(buildString(item));

            if (++i < items.size()) {
                builder.append(", ");
            }
        }
    }
}
