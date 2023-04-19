package com.sbbsystems.flink.tasks.embedded;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

public class PipelineFunctionProvider implements StatefulFunctionProvider {

    public StatefulFunction functionOfType(FunctionType type) {
        return new PipelineFunction();
    }
}
