package com.sbbsystems.statefun.tasks.e2e;

import com.sbbsystems.statefun.tasks.PipelineFunctionModule;
import com.sbbsystems.statefun.tasks.configuration.PipelineConfiguration;
import com.sbbsystems.statefun.tasks.testmodule.IoIdentifiers;
import com.sbbsystems.statefun.tasks.testmodule.TestPipelineRouter;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

import java.util.Map;

public class EndToEndFunctionModule implements StatefulFunctionModule {
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        var mainModule = new PipelineFunctionModule();
        var configuration = PipelineConfiguration.of("e2e/embedded_pipeline", "example/kafka-generic-egress");

        mainModule.configure(configuration, binder);
        binder.bindFunctionProvider(RemoteFunction.FUNCTION_TYPE, unused -> new RemoteFunction());
        binder.bindIngressRouter(IoIdentifiers.REQUEST_INGRESS, new TestPipelineRouter());
    }
}
