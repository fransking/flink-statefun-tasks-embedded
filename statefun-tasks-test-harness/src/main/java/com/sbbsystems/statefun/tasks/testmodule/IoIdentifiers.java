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
package com.sbbsystems.statefun.tasks.testmodule;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

public final class IoIdentifiers {

    public static final String NAMESPACE = "test-harness";
    public static final IngressIdentifier<TypedValue> REQUEST_INGRESS = new IngressIdentifier<>(TypedValue.class, NAMESPACE, "in");
    public static final EgressIdentifier<TypedValue> RESULT_EGRESS = new EgressIdentifier<>("example", "kafka-generic-egress", TypedValue.class);
    public static final FunctionType ECHO_FUNCTION_TYPE = new FunctionType(NAMESPACE, "remote_function");
}
