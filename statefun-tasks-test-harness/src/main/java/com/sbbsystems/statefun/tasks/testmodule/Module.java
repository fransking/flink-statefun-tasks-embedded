package com.sbbsystems.statefun.tasks.testmodule;

import com.sbbsystems.statefun.tasks.PipelineFunctionModule;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

import java.util.Map;

public class Module implements StatefulFunctionModule {
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        var mainModule = new PipelineFunctionModule();
        mainModule.configure(globalConfiguration, binder);
        binder.bindFunctionProvider(IoIdentifiers.REMOTE_FUNCTION_TYPE, unused -> new TestRemoteFunction());
        binder.bindFunctionProvider(IoIdentifiers.EMBEDDED_FUNCTION_TYPE, unused -> new TempPipelineFunction());
        binder.bindIngressRouter(IoIdentifiers.REQUEST_INGRESS, new TestPipelineRouter());
    }
}

