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
