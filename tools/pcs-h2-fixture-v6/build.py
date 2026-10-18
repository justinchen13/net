#!/usr/bin/env python3
"""CI-only build of frozen synthetic fixture; never execute its network main."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
OUT = ROOT / "dist/pcs-h2-fixture-v6"


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def utc():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def main():
    contract = json.loads((HERE / "input-manifest.json").read_text())
    # Reject accidental execution on the production host before invoking Go.
    require(os.environ.get("GITHUB_ACTIONS") == "true", "GitHub-hosted CI only")
    require(os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted", "not a hosted runner")
    require(os.environ.get("GITHUB_REPOSITORY") == contract["ci_repository"], "wrong repository")
    require(os.environ.get("GITHUB_REF") == "refs/heads/" + contract["ci_branch"], "wrong branch")
    OUT.mkdir(parents=True, exist_ok=False)
    result = {"schema": 1, "status": "RUNNING", "started_utc": utc(),
              "scope": contract["scope"], "network_fixture_executed": False,
              "proxy_built_or_deployed": False, "steps": [],
              "source_manifest_sha256": contract["source_manifest_sha256"],
              "net_base_commit": contract["net_base_commit"],
              "workflow_commit": os.environ.get("GITHUB_SHA"),
              "run_id": os.environ.get("GITHUB_RUN_ID"),
              "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
              "runner": {k: os.environ.get(k) for k in
                         ("RUNNER_OS", "RUNNER_ARCH", "RUNNER_ENVIRONMENT", "ImageOS", "ImageVersion")}}
    env = os.environ.copy()
    env.update({k: contract[k] for k in ("GOMAXPROCS", "GOMEMLIMIT", "GOTOOLCHAIN", "GOWORK", "GOAMD64")})
    env.update(CGO_ENABLED="0", GOOS="linux", GOARCH="amd64", GOFLAGS="",
               GOPROXY="https://proxy.golang.org", GOSUMDB="sum.golang.org")
    deadline = time.monotonic() + 720

    def run(name, argv):
        left = deadline - time.monotonic()
        require(left > 0, "build time budget exhausted")
        step = {"name": name, "command": argv, "started_utc": utc()}
        result["steps"].append(step)
        try:
            proc = subprocess.run(argv, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, timeout=left, check=False)
            (OUT / (name + ".log")).write_bytes(proc.stdout)
            step.update(exit_code=proc.returncode, finished_utc=utc())
            require(proc.returncode == 0, name + " failed; see artifact log")
            return proc.stdout.decode("utf-8")
        except subprocess.TimeoutExpired as exc:
            (OUT / (name + ".log")).write_bytes(exc.stdout or b"")
            step.update(finished_utc=utc(), timeout=True)
            raise

    def verify_inputs():
        for name, expected in contract["module_sha256"].items():
            require(sha(ROOT / name) == expected, name + " changed")
        for name, expected in contract["source_sha256"].items():
            require(sha(HERE / name) == expected, name + " changed")

    try:
        verify_inputs()
        head = run("git-head", ["git", "rev-parse", "HEAD"]).strip()
        require(head == result["workflow_commit"], "checkout differs from event SHA")
        changed = run("base-source-diff", ["git", "diff", "--name-only",
                      contract["net_base_commit"], "HEAD", "--"]).splitlines()
        allowed_workflow = ".github/workflows/pcs-h2-fixture-v6.yml"
        require(all(p == allowed_workflow or p.startswith("tools/pcs-h2-fixture-v6/")
                    for p in changed), "module source differs from pinned base")
        run("tracked-clean", ["git", "diff", "--exit-code", "HEAD", "--"])
        version = run("go-version", ["go", "version"]).strip()
        require(version == "go version go1.25.6 linux/amd64", "wrong Go toolchain")
        goenv = json.loads(run("go-env", ["go", "env", "-json", "GOVERSION", "GOOS", "GOARCH",
                                         "GOAMD64", "CGO_ENABLED", "GOTOOLCHAIN", "GOWORK", "GOMOD", "GOROOT"]))
        require(goenv["GOMOD"] == str(ROOT / "go.mod"), "wrong module root")
        result["toolchain"] = goenv
        result["toolchain_go_binary_sha256"] = sha(Path(goenv["GOROOT"]) / "bin/go")
        fixture = str(HERE / "fixture.go")
        tests = str(HERE / "fixture_test.go")
        # Fetch only packages used by this fixture, without executing or compiling it.
        # "go mod download all" would also fetch unused modules and expand frozen go.sum.
        run("fixture-dependencies", ["go", "list", "-mod=readonly", "-deps", "-json", fixture])
        run("modules-verify", ["go", "mod", "verify"])
        verify_inputs()
        env.update(GOPROXY="off", GOSUMDB="off")
        # Record the same package/module closure offline, not the entire module graph.
        run("modules-list", ["go", "list", "-mod=readonly", "-deps", "-json", fixture])
        test_output = run("pure-tests", ["go", "test", *contract["test_flags"], fixture, tests])
        events = [json.loads(line) for line in test_output.splitlines() if line.strip()]
        top = contract["expected_top_test"]
        cases = [e["Test"] for e in events if e.get("Action") == "pass"
                 and e.get("Test", "").startswith(top + "/")]
        require(len(cases) == len(set(cases)) == contract["expected_table_cases"], "expected 15 unique passing enum cases")
        require(any(e.get("Action") == "pass" and e.get("Test") == top for e in events), "top test did not pass")
        require(any(e.get("Action") == "pass" and not e.get("Test") for e in events), "test package did not pass")
        require(not any(e.get("Action") in ("fail", "skip") for e in events), "test failure or skip")
        result["pure_tests"] = {"status": "PASS", "table_cases": len(cases), "case_names": cases}
        binary = OUT / "fixture"
        run("build", ["go", "build", *contract["build_flags"], "-o", str(binary), fixture])
        run("binary-build-info", ["go", "version", "-m", str(binary)])
        verify_inputs()
        run("tracked-clean-after", ["git", "diff", "--exit-code", "HEAD", "--"])
        result.update(status="PASS", binary_sha256=sha(binary), binary_bytes=binary.stat().st_size)
    except Exception as exc:
        result.update(status="FAIL", failure={"type": type(exc).__name__, "reason": str(exc)[:1000]})
    finally:
        for name in ("fixture.go", "fixture_test.go", "input-manifest.json", "actions-lock.json", "build.py", "README.md"):
            shutil.copyfile(HERE / name, OUT / name)
        shutil.copyfile(ROOT / "go.mod", OUT / "go.mod")
        shutil.copyfile(ROOT / "go.sum", OUT / "go.sum")
        result["finished_utc"] = utc()
        result["files_sha256"] = {p.name: sha(p) for p in sorted(OUT.iterdir()) if p.is_file()}
        (OUT / "build-result.json").write_text(json.dumps(result, indent=2) + "\n")
        checksums = "".join(sha(p) + "  " + p.name + "\n" for p in sorted(OUT.iterdir()) if p.is_file())
        (OUT / "SHA256SUMS").write_text(checksums)
    print(json.dumps({k: result.get(k) for k in ("status", "binary_sha256", "pure_tests", "failure")}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
