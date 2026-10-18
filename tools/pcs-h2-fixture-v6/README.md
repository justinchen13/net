# Frozen HTTP2 v6 fixture build

This branch only compiles a synthetic client fixture and runs its 15 pure
`parseUploadDelay` table cases. It does not execute the fixture's network main,
build a proxy, deploy anything, or contain production credentials. It does not
include any Hysteria ACK-height or AnyTLS route experiment.

The copied `fixture.go` and `fixture_test.go` are byte-identical to the frozen
v6 inputs. `input-manifest.json` records their SHA-256 values, the originating
manifest SHA-256, the exact x/net base commit, and unchanged go.mod/go.sum hashes.
The build rejects changes outside this CI workflow and fixture directory relative
to that base. No general x/net test suite is invoked.

The workflow runs only on pushes to `pcs/ci-h2-fixture-v6` in `justinchen13/net`.
It has `contents: read`, no configured secrets, no manual/PR trigger, no deployment
step, a 15-minute job limit, and 7-day artifacts. Checkout does not persist Git
credentials. The official actions are pinned to verified commit SHAs; release,
tag API and exact action.yml references are in `actions-lock.json`.

Go is exactly 1.25.6, linux/amd64, CGO disabled, GOAMD64=v1 and GOTOOLCHAIN=local.
Dependencies are fetched at the existing locked versions via the public Go proxy
with sum.golang.org verification. After `go mod verify`, test/build run with
GOPROXY/GOSUMDB off. Inputs are checked again afterwards.

The local builder's test flags were `-p=1 -count=1 -timeout=20s -mod=readonly`;
CI adds only `-json` to require all 15 distinct cases and the parent/package to
pass without skips. Build flags stay `-p=1 -mod=readonly -buildvcs=false -trimpath`.
No fixture binary is executed, including after it is built.

CI uses the standard Ubuntu 24.04 hosted runner, not the local Alpine cache image
`9a047b56...`. Local cgroup limits (.35 CPU, 512 MiB, no additional swap) are not
reproduced. GOMAXPROCS=1/GOMEMLIMIT=320MiB are retained, but CI resource use is not
a proxy-performance result. The runner image, toolchain binary hash, module list,
source hashes, commands, tests, binary build info and output SHA-256 are retained.
Matching compiler version and flags alone do not prove byte-identical binaries
across builder environments.

The artifact contains `fixture`, the frozen sources, locked module files,
`build-result.json`, `SHA256SUMS` and build logs. Download consumers must first
verify the workflow commit/run provenance, require build status PASS and 15 cases,
then check hashes and build info. The artifact has independent CI provenance:
it is not the old local `build-result.json` and must not be passed off as one.
A separately reviewed adapter/authorization is required before any later runtime
experiment; both experiment arms must use the same accepted fixture binary.

Current preparation status: static validation only. No local Go compilation,
workflow push or CI execution was performed while preparing these files.
