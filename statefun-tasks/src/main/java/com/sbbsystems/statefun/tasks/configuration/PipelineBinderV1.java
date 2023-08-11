package com.sbbsystems.statefun.tasks.configuration;

import com.sbbsystems.statefun.tasks.PipelineFunctionModule;
import com.sbbsystems.statefun.tasks.PipelineFunctionProvider;
import com.sbbsystems.statefun.tasks.batchcallback.CallbackFunctionProvider;
import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineBinderV1 implements ComponentBinder {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineFunctionModule.class);

    public static final PipelineBinderV1 INSTANCE = new PipelineBinderV1();
    public static final TypeName KIND_TYPE = TypeName.parseFrom("io.statefun_tasks.v1/pipeline");

    @Override
    public void bind(ComponentJsonObject componentJsonObject, StatefulFunctionModule.Binder binder) {
        var configuration = PipelineConfiguration.fromNode(componentJsonObject.specJsonNode());
        bind(configuration, binder);
    }

    public void bind(PipelineConfiguration configuration, StatefulFunctionModule.Binder binder) {
        try {
            var pipelineFunctionType = new FunctionType(configuration.getNamespace(), configuration.getType());
            var callbackFunctionType = new FunctionType(configuration.getNamespace(), configuration.getCallbackType());

            binder.bindFunctionProvider(pipelineFunctionType, PipelineFunctionProvider.of(configuration, callbackFunctionType));
            binder.bindFunctionProvider(callbackFunctionType, CallbackFunctionProvider.of(configuration, pipelineFunctionType));

            LOG.info("Embedded pipeline function registered");
        } catch (IllegalStateException e) {
            LOG.error("Error registering embedded pipeline function", e);
        }
    }
}
