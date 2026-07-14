# Participant Query Store

## Connectivity test

Test dev loop is working properly:

```text
mill experiments.connectivity.testConnection
```

This will deploy `experiments/ping-pong` dar into local sandbox server and run `experiments/connectivity` application against it.

## Start/stop daml sandbox in a daml module

To start a sandbox with daml package deployed:

```text
mill experiments.ping-pong.start
```

To execute an init-script:
```text
mill experiments.ping-pong.run "PingPong:setup"
```

To stop sandbox:
```text
mill experiments.ping-pong.stop
```

To run sandbox in a watch mode, i.e. when package is redeployed on source changes automatically:
```text
mill -w experiments.ping-pong.run "PingPong:setup"
```

## Scalafmt

Check and reformat:

```text
mill all.checkScalafmt
mill all.reformat
```

## Functional tests

Functional tests run against the whole environment (canton, postgres, scribe) running in docker containers, JVM test does this job roughly:

1. Start canton in docker (+ another container with postgres as a backing store)
2. Start postgres for PQS
3. Start scribe with options specific to the test
4. Analyse PQS output and/or query postgres to verify results

Before starting the tests, a docker image with scribe is built. The tests are ran against:
 - a just built scribe image
 - canton, postgres -- specified either by 
   - the daml and Canton versions defined in object `V` in `mill-build/src/millbuild/package.scala`, and `forkEnv` function in `build.sc`
   - or by the environment variables
     - `SCRIBE_POSTGRESVERSION`
     - `SCRIBE_DAMLSDKVERSION`
     - `SCRIBE_CANTONVERSION`
     - `SCRIBE_CANTONPROTOCOLVERSION`
     - `SCRIBE_DAMLLFTARGET`
   - It should be also possible to select version using commandline options, but it is not used anywhere. Parsing env vars/commandline options is generated using zio-config-magnolia: https://zio.dev/zio-config/
   
The tests use custom test runner FTFramework. This test runner splits tests into groups that use the same environment (`def shared` in test class). For each group
 - test environment is set up once,
 - all the tests from the group are run on this environment.
 
Tests are run in parallel, there are two parameters:
 - pools -- a number of groups (as defined above) of the tests that are ran concurently,
 - lanes -- a number of tests that are run in parallel within a group.
 
It looks like the initial idea was that number of pools controls the memory usage as most tests set up some docker containers within the shared context.
The number of lanes intended to control CPU usage without much impact on memory. 
This is not true though, as there are tests that set up ad-hoc docker containers within run, so increasing number of lanes may exhaust memory as well.

To specify those parameters for the tests

- `mill scribe.functest.test --pools 2 --lanes 2`
- `mill scribe.functest.testOnly com.digitalasset.scribe.features.filtering.daml3.ContractFilteringSpec  -- --pools 1 --lanes 2`

Please note the double dash in the testOnly command.
The defaults are here: `src/com/digitalasset/scribe/functest/sbt/FTSpec.scala`.

### Debugging Functional Tests FAQ

#### Q: How do I inspect the temp directory?

Set the `FT_KEEP_TEMP=true` env var before running tests. This prevents the temp directory from being deleted after the test run, allowing you to inspect its contents.

#### Q: How do I enable Canton logging for functional tests?

By default, functional tests do not output Canton container logs.
Set `FT_CANTON_LOG=true` to enable Canton log output in the console.

#### Q: How do I observe the full gRPC error messages within a ZIO layer?

```shell
val layer: ZIO.ZLayer = ???

layer.tapError {
  case exn: io.grpc.StatusException =>
    ZIO.succeed(println(s"StatusException: $exn [${exn.getTrailers()}]"))
    
  case exn =>
    ZIO.succeed(println(s"Unknown: $exn"))
}
```
This code will console log the base64 encoded bytes for the full protobuf error message. A base64 decode allows the error
message string based details to be observed.

### Compatibility test matrix

In `apps/compatibility` there are scripts to run a series of functional tests against different versions of canton, defined in `compatibility/parameters.csv`. This script is runned in CI.

