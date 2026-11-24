# Release of PQS PQS_VERSION

PQS PQS_VERSION has been released on RELEASE_DATE

## Summary

_Write summary of release_

## SQL Migration

## What's New

### Security

- The Docker image and the dpm component disable the JVM's hardware-accelerated Base64 decoder (`-XX:-UseBASE64Intrinsics`) to mitigate CVE-2026-114938 (Java4Shell). The image sets the flag in `JAVA_TOOL_OPTIONS`, the dpm component in its `jvm-args`. If you override `JAVA_TOOL_OPTIONS`, or run `pqs.jar` directly, add the flag yourself or use a patched JDK.
- JDK base image has been upgraded to 21.0.13.1_2, incorporating several key security patches
