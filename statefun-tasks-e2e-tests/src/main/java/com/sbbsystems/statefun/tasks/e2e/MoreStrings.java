/*
 * Copyright [2023] [Frans King, Luke Ashworth]
 * Copyright [2026] [Frans King]
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
import com.google.protobuf.BoolValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.sbbsystems.statefun.tasks.generated.*;

import java.util.List;
import java.util.Map;

public class MoreStrings {

    public static String asString(Any value)
            throws InvalidProtocolBufferException {

        return buildString(value).toString();
    }

    public static String asString(Value value)
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

        } else if (value.is(TupleOfValue.class)) {
            builder.append("(");
            appendValuesTo(builder, value.unpack(TupleOfValue.class).getItemsList());
            builder.append(")");

        } else if (value.is(ArrayOfValue.class)) {
            builder.append("[");
            appendValuesTo(builder, value.unpack(ArrayOfValue.class).getItemsList());
            builder.append("]");

        } else if (value.is(ValueArgsAndKwargs.class)) {
            builder.append(buildString(value.unpack(ValueArgsAndKwargs.class)));

        } else if (value.is(MapOfStringToAny.class)) {
            var map = value.unpack(MapOfStringToAny.class).getItemsMap();
            builder.append("{");
            appendTo(builder, map);
            builder.append("}");

        } else if (value.is(MapOfStringToValue.class)) {
            var map = value.unpack(MapOfStringToValue.class).getItemsMap();
            builder.append("{");
            appendValuesTo(builder, map);
            builder.append("}");

        } else if (value.is(Int32Value.class)) {
            builder.append(value.unpack(Int32Value.class).getValue());
        } else if (value.is(StringValue.class)) {
            builder.append(value.unpack(StringValue.class).getValue());
        } else if (value.is(BoolValue.class)) {
            builder.append(value.unpack(BoolValue.class).getValue());
        } else if (value.is(TaskException.class)) {
            builder.append(value.unpack(TaskException.class).getExceptionMessage());
        } else if (value.is(TaskStatus.class)) {
            builder.append(value.unpack(TaskStatus.class).getValue());
        } else if (value.is(TaskResult.class)) {
            builder.append(buildString(value.unpack(TaskResult.class).getResult()));
        } else if (value.is(TaskRequest.class)) {
            builder.append(buildString(value.unpack(TaskRequest.class).getRequest()));
        } else if (value.is(Pipeline.class)) {
            builder.append(value.unpack(Pipeline.class));
        } else if (value.is(Value.class)) {
            builder.append(buildString(value.unpack(Value.class)));
        }

        return builder;
    }

    private static StringBuilder buildString(Value value)
            throws InvalidProtocolBufferException {

        var builder = new StringBuilder();

        switch (value.getKindCase()) {
            case NONE_VALUE:
                builder.append("None");
                break;
            case DOUBLE_VALUE:
                builder.append(value.getDoubleValue());
                break;
            case INT_VALUE:
                builder.append(value.getIntValue());
                break;
            case BOOL_VALUE:
                builder.append(value.getBoolValue());
                break;
            case STRING_VALUE:
                builder.append(value.getStringValue());
                break;
            case BYTES_VALUE:
                builder.append(value.getBytesValue().toStringUtf8());
                break;
            case TUPLE_VALUE:
                builder.append("(");
                appendValuesTo(builder, value.getTupleValue().getItemsList());
                builder.append(")");
                break;
            case ARRAY_VALUE:
                builder.append("[");
                appendValuesTo(builder, value.getArrayValue().getItemsList());
                builder.append("]");
                break;
            case MAP_VALUE:
                builder.append("{");
                appendValuesTo(builder, value.getMapValue().getItemsMap());
                builder.append("}");
                break;
            case ANY_VALUE:
                builder.append(buildString(value.getAnyValue()));
                break;
            default:
                break;
        }

        return builder;
    }

    private static StringBuilder buildString(ValueArgsAndKwargs valueArgsAndKwargs)
            throws InvalidProtocolBufferException {

        var builder = new StringBuilder();
        builder.append("(");
        appendValuesTo(builder, valueArgsAndKwargs.getArgs().getItemsList());
        builder.append(")");
        if (valueArgsAndKwargs.getKwargs().getItemsCount() > 0) {
            builder.append(" {");
            appendValuesTo(builder, valueArgsAndKwargs.getKwargs().getItemsMap());
            builder.append("}");
        }
        return builder;
    }

    private static void appendTo(StringBuilder builder, Map<String, Any> map)
            throws InvalidProtocolBufferException {

        var i = 0;
        for (var key : map.keySet()) {
            builder.append(key);
            builder.append(": ");
            builder.append(buildString(map.get(key)));

            if (++i < map.size()) {
                builder.append(", ");
            }
        }
    }

    private static void appendTo(StringBuilder builder, List<Any> items)
            throws InvalidProtocolBufferException {

        var i = 0;
        for (var item : items) {
            builder.append(buildString(item));

            if (++i < items.size()) {
                builder.append(", ");
            }
        }
    }

    private static void appendValuesTo(StringBuilder builder, Map<String, Value> map)
            throws InvalidProtocolBufferException {

        var i = 0;
        for (var key : map.keySet()) {
            builder.append(key);
            builder.append(": ");
            builder.append(buildString(map.get(key)));

            if (++i < map.size()) {
                builder.append(", ");
            }
        }
    }

    private static void appendValuesTo(StringBuilder builder, List<Value> items)
            throws InvalidProtocolBufferException {

        var i = 0;
        for (var item : items) {
            builder.append(buildString(item));

            if (++i < items.size()) {
                builder.append(", ");
            }
        }
    }
}
