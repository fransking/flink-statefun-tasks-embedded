# flink-statefun-tasks-embedded
Embedded pipeline function for Flink Statefun Tasks

Example module.yaml:


    version: "3.0"
    module:
      meta:
        type: remote
      spec:
        endpoints:
            ...

        ingresses:
          - ingress:
              meta:
                ...
              spec:
                ...
                topics:
                  - topic: statefun-tasks.requests
                    valueType: io.statefun_tasks.types/statefun_tasks.TaskRequest
                    targets:
                      - example/embedded_pipeline
                  - topic:  statefun-tasks.actions
                    valueType: io.statefun_tasks.types/statefun_tasks.TaskActionRequest
                    targets:
                      - example/embedded_pipeline

        egresses:
          - egress:
              meta:
                type: io.statefun.kafka/egress
                id: example/kafka-generic-egress
              spec: 
                ...

        pipelines:
          - pipeline:
              meta:
                id: example/embedded_pipeline               # function namespace/type
              spec:
                stateExpiration: PT1M                       # state expiration (ISO-8601)
                egress: example/kafka-generic-egress        # task response egress
                eventsEgress: example/kafka-generic-egress  # events egress
                eventsTopic: statefun-tasks.events          # events topic
