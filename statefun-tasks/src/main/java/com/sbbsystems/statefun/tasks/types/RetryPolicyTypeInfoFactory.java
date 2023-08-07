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
package com.sbbsystems.statefun.tasks.types;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.lang.reflect.Type;
import java.util.Map;

public class RetryPolicyTypeInfoFactory extends TypeInfoFactory<RetryPolicy> {
    @Override
    public TypeInformation<RetryPolicy> createTypeInfo(Type type, Map<String, TypeInformation<?>> map) {
        return Types.POJO(RetryPolicy.class,
                Map.of(
                        "retryFor", Types.LIST(Types.STRING),
                        "maxRetries", Types.INT,
                        "delayMs", Types.FLOAT,
                        "exponentialBackOff", Types.BOOLEAN
                ));
    }
}
