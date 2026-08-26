# Release process

## How to create a snapshot release

Snapshot versions are continuously published by the CI, after each merge to `main` or release branches (e.g. `release-line-3.5`).

## How to create a stable release

Stable releases must be triggered exclusively from the release branches (e.g. `release-line-3.5`).

In preparation for the release, open a PR to update the Canton and daml versions to their latest stable versions,
in the [build configuration](mill-build/src/millbuild/package.scala), and the [matrix compatibility tests](modules/compatibility/parameters.csv).

Once the PR is merged into the release branch, wait for the `build-ship` workflow to run successfully.

To trigger the stable release, create a Github release from the Github UI:
- Go to https://github.com/digital-asset/participant-query-store/releases
- Click on `Draft a new release`
- Open the `Select tag` drop-down
    - Click on `Create a new tag`
    - Type the tag of the form `v3.5.1`, where `3.5` is the target release line and `1` is the next available increment.
- Open the `Target` drop-down and select the corresponding release branch. To release `v3.5.1` you should select `release-line-3.5`.
- As release title, use the tag version: `v3.5.1`
- Click on `Generate release notes`. Verify that the previous tag is correct. You should see something like:
```
**Full Changelog**: https://github.com/digital-asset/participant-query-store/compare/v3.5.0...v3.5.1
```
- For a backport release, uncheck the `Set as the latest release` box
- Submit the release by clicking on `Publish release`

Github creates the version tag on the release branch.
It triggers the `sanctioned-release` workflow, on that branch, which will publish scribes JAR, docker image and dpm component. 

## How to create a release branch

Before updating the base version of the main branch, create a release branch for the current base version.

Create the release branch from main and push it.
```
git fetch origin
git checkout -b release-line-3.5 origin/main
git push origin -u HEAD
```

Check every base version passed as argument to `_gen_build_version`, the component tag in [Makefile](Makefile),
the `daml` and `canton` version in [mill-build/src/deps/package.scala](mill-build/src/millbuild/package.scala).
They should match the base version of the release.
If that's not the case, update them as described below.

## How to update the base version

Soon after creating a release branch, you should increment the base version of the main branch.

For example, to update the base version from 3.6 to 3.7, create a PR with the following changes:
- update the BASE_VERSION in [_gen_build_version](.scaffold/bin),
[docker/images/scribe/Makefile](docker/images/scribe/Makefile),

```diff
-BASE_VERSION=3.6
+BASE_VERSION=3.7
```
- update the chart version in [VERSION](VERSION)
```diff
-3.6.0-pre
+3.7.0-pre
```
- update the dpm component tag in [Makefile](Makefile)
```diff
-    --extra-tags 3.6 \
+    --extra-tags 3.7 \
```
- update the `damlc` and `canton` versions in [mill-build/src/deps/package.scala](mill-build/src/millbuild/package.scala)
```diff
-    val damlc = "3.6.0-snapshot.20260501.14683.0.v4cff1caf"
+    val damlc = "3.7.0-snapshot.20261020.14344.0.vac465d36"

-    val canton = "3.6.0-snapshot.20260501.14683.0.v4cff1caf"
+    val canton = "3.7.0-snapshot.20261020.14344.0.vac465d36"
```
- update the `SCRIBE_DAMLSDKVERSION` and `SCRIBE_CANTONVERSION` versions in the [matrix compatibility tests](modules/compatibility/parameters.csv)
- Add `Daml36` config in [modules/scribe/functest/com/digitalasset/scribe/services/daml/CantonConf.scala](modules/scribe/functest/com/digitalasset/scribe/services/daml/CantonConf.scala)

