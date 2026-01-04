/*
 * Copyright [2025] [Frans King]
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
package com.sbbsystems.statefun.tasks.graph.v2;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.lang.reflect.Type;
import java.util.Map;

public class MapTypeInfoFactory<K, V> extends TypeInfoFactory<Map<K, V>> {
    @Override
    public TypeInformation<Map<K, V>> createTypeInfo(Type type, Map<String, TypeInformation<?>> map) {
        TypeInformation<?> keyType = map.get("K");
        if (keyType == null) {
            throw new InvalidTypesException("Key type unknown");
        }

        TypeInformation<?> valueType = map.get("V");
        if (valueType == null) {
            throw new InvalidTypesException("Value type unknown");
        }

        //noinspection unchecked
        return Types.MAP((TypeInformation<K>) keyType, (TypeInformation<V>) valueType);
    }
}
