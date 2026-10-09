# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The `type: locust` hooks (see the package docstring for the lifecycle).

The runner Job wraps benchmarking/locust/runner.py, which drives locust
(and boomer for glutton tests) and uploads its own results.
"""

import os
from typing import Any

from util import build_and_push

TEST_TYPE = "locust"


def _has_flag(flags: list[Any], flag: str) -> bool:
    prefix = f"{flag}="
    return any(str(f) == flag or str(f).startswith(prefix) for f in flags)


def validate(test: dict[str, Any]) -> None:
    name = test.get("name")
    for field in ("file", "duration", "users"):
        if field not in test:
            raise ValueError(f"locust test {name!r} missing {field!r}")
    if "cpuCores" in test:
        raw_cores = test["cpuCores"]
        if isinstance(raw_cores, bool):
            raise ValueError(
                f"locust test {name!r} has invalid cpuCores {raw_cores!r} (want int >= 0)"
            )
        try:
            cores = int(str(raw_cores).strip())
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"locust test {name!r} has invalid cpuCores {raw_cores!r} (want int >= 0)"
            ) from e
        if cores < 0 or str(cores) != str(raw_cores).strip():
            raise ValueError(
                f"locust test {name!r} has invalid cpuCores {raw_cores!r} (want int >= 0)"
            )
        flags = list(test.get("flags", []))
        if not _has_flag(flags, "--cpu-cores"):
            flags.extend(["--cpu-cores", str(cores)])
            test["flags"] = flags
    if "cpuDutyCycle" in test:
        raw_duty = test["cpuDutyCycle"]
        if isinstance(raw_duty, bool):
            raise ValueError(
                f"locust test {name!r} has invalid cpuDutyCycle {raw_duty!r} (want float in [0.0, 1.0])"
            )
        try:
            duty = float(str(raw_duty).strip())
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"locust test {name!r} has invalid cpuDutyCycle {raw_duty!r} (want float in [0.0, 1.0])"
            ) from e
        if not (0.0 <= duty <= 1.0):
            raise ValueError(
                f"locust test {name!r} has invalid cpuDutyCycle {raw_duty!r} (want float in [0.0, 1.0])"
            )
        flags = list(test.get("flags", []))
        if not _has_flag(flags, "--cpu-duty-cycle"):
            flags.extend(["--cpu-duty-cycle", str(raw_duty).strip()])
            test["flags"] = flags


def build_image(commit: str) -> str:
    """Build & push the locust runner image. It must live in the same
    project as the test cluster so the runner Job can pull it."""
    return build_and_push(
        f"{os.environ['KO_DOCKER_REPO']}/locust-test:{commit}",
        "benchmarking/locust/Dockerfile",
    )


def pre_test(test: dict[str, Any]) -> None:
    """Nothing to shape on the cluster before a locust run."""


def job_tmpl(manifests_dir: str) -> str:
    return os.path.join(manifests_dir, "runner-job.yaml.tmpl")


# Runner pod requests when a test sets none. Enough for the glutton and
# sweperf suites at their usual user counts; a boomer-driven class at
# thousands of users (agentsession marshals a 32Mi ingest payload per
# in-flight op) needs runnerCpu / runnerMemory raised in tests.yaml.
DEFAULT_RUNNER_CPU = "500m"
DEFAULT_RUNNER_MEMORY = "512Mi"


def job_subs(test: dict[str, Any]) -> dict[str, Any]:
    return {
        "TEST_FILE": test["file"],
        "DURATION": test["duration"],
        "USERS": test["users"],
        "RUNNER_CPU": test.get("runnerCpu", DEFAULT_RUNNER_CPU),
        "RUNNER_MEMORY": test.get("runnerMemory", DEFAULT_RUNNER_MEMORY),
    }
