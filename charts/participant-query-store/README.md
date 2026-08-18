# Chart Documentation

This document provides a comprehensive guide to the `values.yaml` configuration for the PQS Helm chart. It explains how each section of the values file directly informs the generated Kubernetes manifests, specifically the `Deployment` and `ConfigMap`.

## Table of Contents
1. [Standard Kubernetes Configurations](#1-standard-kubernetes-configurations)
2. [Secret Mapping Mechanism](#2-secret-mapping-mechanism)
3. [PQS Configuration & HOCON Generation](#3-pqs-configuration--hocon-generation)

---

## 1. Standard Kubernetes Configurations

These top-level sections in the `values.yaml` control the standard lifecycle, identity, and resource constraints of the Kubernetes Pods.

### `image`
* **Description:** Defines the container image registry, repository, and tag used for the PQS application.
* **How it informs `deployment.yaml`:** Mapped directly to the `spec.template.spec.containers[0].image` field. If the tag is omitted, it defaults to the `.Chart.AppVersion`.
* **Example `values.yaml` Configuration:**
  ```yaml
  image:
    repo: europe-docker.pkg.dev/da-images/public-private-all/docker
    tag: "3.4.5"
  ```

### `serviceAccount`
* **Description:** Defines the Kubernetes ServiceAccount name and its annotations (e.g., for workload identity mapping).
* **How it informs `deployment.yaml`:** The `name` is mapped to `spec.template.spec.serviceAccount` (or `serviceAccountName`). Annotations are useful for cloud provider IAM integration.
* **Example `values.yaml` Configuration:**
  ```yaml
  serviceAccount:
    name: pqs-sa
    annotations:
      iam.gke.io/gcp-service-account: pqs-service-account@my-project.iam.gserviceaccount.com
  ```

### `pod`
* **Description:** Configuration specifically targeted at the Pod level, such as custom annotations.
* **How it informs `deployment.yaml`:** Appended to `spec.template.metadata.annotations` using `toYaml`. Useful for metrics scraping or specialized routing logic.
* **Example `values.yaml` Configuration:**
  ```yaml
  pod:
    annotations:
      prometheus.io/scrape: "true"
      prometheus.io/port: "8091"
  ```

### `containers`
* **Description:** Defines container-level specifications of `requests`, `limits`, `readinessProbe`, and `livnessProbe` to ensure proper cluster scheduling and prevent resource starvation.
* **How it informs `deployment.yaml`:** Injected directly into the `resources`, `readinessProbe`, and `livenessProbe` block of the `pqs` container.
* **NOTE:** Port matches what is defined in the `values.yaml` file in the `pqs.health.port` block and needs to match it
* **Example `values.yaml` Configuration:**
  ```yaml
  containers:
    resources:
      requests:
        memory: 256M
        cpu: 500m
      limits:
        memory: 3G
        cpu: 1000m
    readinessProbe:
      httpGet:
        path: /livez
        port: 8091
      initialDelaySeconds: 60
      periodSeconds: 15
    livenessProbe:
      httpGet:
        path: /livez
        port: 8091
      initialDelaySeconds: 60
      periodSeconds: 30
  ```

### `autoPrune`
* **Description:** Enable PQS auto pruning as per defined schedule and retention period
* **How it informs `deployment.yaml`:** Will setup a kubernetes cron job to run pruning job as per defined configuration.
* **Example `values.yaml` Configuration:**
  ```yaml
  autoPrune:
    # Enable auto pruning (disabled by default)
    enabled: true

    # Define when the pqs pruning job will run format: POSIX 5 field cron schedule
    cronSchedule: "0 * * * *"

    # ISO 8601 - Default prunes >30 days
    maxAge: "P30D"
  ```

### `initContainers`
* **Description:** Allows injection of custom initialization containers that run to completion before the main PQS app starts.
* **How it informs `deployment.yaml`:** Rendered directly into `spec.template.spec.initContainers`.
* **Example `values.yaml` Configuration:**
  ```yaml
  initContainers:
    - name: wait-for-postgres
      image: busybox:1.28
      command: ['sh', '-c', 'until nc -z postgres 5432; do echo waiting for db; sleep 2; done;']
  ```

### `env`
* **Description:** Allows injection of raw environment variables directly into the PQS container.
* **How it informs `deployment.yaml`:** Rendered directly into the `env` block of the `pqs` container definition alongside the auto-generated secret variables and JMX settings.
* **Example `values.yaml` Configuration:**
  ```yaml
  env:
    - name: JAVA_OPTS
      value: " -Dfile.encoding=UTF-8 -Djava.io.tmpdir=/tmp/myapp"
  ```

---

## 2. Secret Mapping Mechanism

The chart handles sensitive data securely without exposing them in plaintext configurations. 

### `secrets`
* **Description:** Maps existing Kubernetes Secrets to environment variables, and then bridges those environment variables into the HOCON configuration file.

* **NOTE:** The secrets need to exist in the Kubernetes Namespace where the PQS Helm Chart is deployed and needs to match the `secretKeyRef`, `name` and `key`.
* **Example `values.yaml` Configuration:**
  ```yaml
  secrets:
  - paths:
    - target.postgres.password
    secretKeyRef:
      name: postgres
      key: postgresPassword
  - paths:
    - pipeline.oauth.clientSecret
    secretKeyRef:
      name: splice-app-validator-ledger-api-auth
      key: client-secret
  ```

---

## 2. PQS Configuration

### `pqs.pipeline`
* **Description:** Configures the data ingestion pipeline, including the data source type, contract filters, ledger read positions, and OAuth authentication specifics.
* **Example `values.yaml` Configuration:**
  ```yaml
  pqs:
    pipeline:
      datasource: TransactionStream
      filter:
        betterWildcard: "true"
        contracts: "*"
        metadata: "*"
        parties: "*"
      ledger:
        start: Latest
        stop: Never
      oauth:
        clientId: oauth-client-id #
        issuer: "https://my.outh.provider/fakeIssuer"
        endpoint: "https://my.outh.provider/fakeIssuer/token"
        preemptExpiry: PT1M
        scope: daml_ledger_api
        parameters:
          audience: "http://fake.audience.com"
  ```

### `pqs.source`
* **Description:** Defines the connection parameters to the participant node (Ledger API), including host, port, authentication type, and connection keep-alive settings.
* **Example `values.yaml` Configuration:**
  ```yaml
  pqs:
    source:
      ledger:
        auth: OAuth
        bufferSize: "128"
        cacheDir: "/tmp/pqs"
        host: participant
        keepAlive:
          time: PT40S
          timeout: PT20S
        port: "5001"
  ```

### `pqs.target`
* **Description:** Configures the destination PostgreSQL database for the data queried from the participant.
* **Example `values.yaml` Configuration:**
  ```yaml
  pqs:
    target:
      encoding:
        excludeNulls: "false"
        int64AsString: "true"
        numericAsString: "true"
      postgres:
        appName: pqs
        bufferSize: "128"
        database: public
        host: postgres
        keepAlive: "true"
        maxConnections: "16"
        username: "postgres"
        port: "5432"
        schema: pqs
        tls:
          mode: Disable
      schema:
        autoApply: "true"
        baseline: "false"
  ```

### `pqs.retry`
* **Description:** Specifies the retry policies and exponential backoff parameters for handling transient failures in the pipeline.
* **Example `values.yaml` Configuration:**
  ```yaml
  pqs:
    retry:
      backoff:
        base: PT1S
        cap: PT1M
        factor: "2.0"
      counter:
        reset: PT10M
  ```

### `pqs.health`
* **Description:** Configures the bind address and port for the application's health check endpoints, which Kubernetes uses for liveness and readiness probes.
* **NOTE:** Needs to remain at 0.0.0.0 for liveness and readiness probes to work
* **Example `values.yaml` Configuration:**
  ```yaml
  pqs:
    health:
      address: "0.0.0.0"
      port: "8091"
  ```

### `pqs.logger`
* **Description:** Controls the logging behavior of the application, such as log levels (e.g., Info, Debug) and output formats (e.g., Plain, JSON).
* **Example `values.yaml` Configuration:**
  ```yaml
  pqs:
    logger:
      format: Plain
      level: Info
      mappings: {}
      pattern: Plain
  ```
  
