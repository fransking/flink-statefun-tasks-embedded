package com.sbbsystems.flink.tasks.embedded.testmodule;

import com.sbbsystems.flink.tasks.embedded.PipelineFunctionModule;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

import java.util.Map;

import static com.sbbsystems.flink.tasks.embedded.testmodule.IoIdentifiers.*;

public class Module implements StatefulFunctionModule {
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        var mainModule = new PipelineFunctionModule();
        mainModule.configure(globalConfiguration, binder);
        binder.bindFunctionProvider(REMOTE_FUNCTION_TYPE, unused -> new TestRemoteFunction());
        binder.bindFunctionProvider(EMBEDDED_FUNCTION_TYPE, unused -> new TempPipelineFunction());
        binder.bindIngressRouter(REQUEST_INGRESS, new TestPipelineRouter());
    }


}

