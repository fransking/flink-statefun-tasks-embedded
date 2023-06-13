package com.sbbsystems.statefun.tasks.e2e;

import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sbbsystems.statefun.tasks.generated.ArrayOfAny;
import com.sbbsystems.statefun.tasks.generated.TupleOfAny;

import java.util.List;

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

        } else if (value.is(Int32Value.class)) {
            builder.append(value.unpack(Int32Value.class).getValue());
        }

        return builder;
    }

    private static void appendTo(StringBuilder builder, List<Any> items) throws InvalidProtocolBufferException {
        var i = 0;
        for (var item: items) {
            builder.append(asString(item));

            if (++i < items.size()) {
                builder.append(", ");
            }
        }
    }
}
