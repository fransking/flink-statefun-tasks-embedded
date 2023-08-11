# flink-statefun-tasks-embedded
Embedded pipeline function for Flink Statefun Tasks

Example module.yaml:

    kind: io.statefun_tasks.v1/pipeline
    spec:
      id: example/embedded_pipeline               # function namespace/type
      stateExpiration: PT1M                       # state expiration (ISO-8601)
      egress: example/kafka-generic-egress        # task response egress
      eventsEgress: example/kafka-generic-egress  # events egress
      eventsTopic: statefun-tasks.events          # events topic
    ---
    kind: io.statefun.endpoints.v2/http
    spec:
      functions: example/worker
      urlPathTemplate: http://worker:8085/statefun
    ---
    kind: io.statefun.kafka.v1/ingress
    spec:
      id: example/worker
      address: kafka-broker:9092
      consumerGroupId: flink-cluster-id
      startupPosition:
        type: earliest
      topics:
        - topic: statefun-tasks.requests
          valueType: io.statefun_tasks.types/statefun_tasks.TaskRequest
          targets:
            - example/embedded_pipeline
        - topic:  statefun-tasks.actions
          valueType: io.statefun_tasks.types/statefun_tasks.TaskActionRequest
          targets:
            - example/embedded_pipeline
    ---
    kind: io.statefun.kafka.v1/egress
    spec:
      id: example/kafka-generic-egress
      address: kafka-broker:9092
      deliverySemantic:
        type: exactly-once
        transactionTimeout: 15min
