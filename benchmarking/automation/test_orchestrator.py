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

"""Unit tests for orchestrator.py: python3 benchmarking/automation/test_orchestrator.py"""

import os
import unittest
from unittest import mock

import yaml

import orchestrator
from testtypes import locust


class DeployWorkloadsTest(unittest.TestCase):
    @mock.patch("orchestrator.run")
    def test_defaults_omit_optional_flags(self, run):
        orchestrator.deploy_workloads()
        run.assert_called_once_with(
            [
                "benchmarking/workloads/deploy.sh",
                "--deploy",
                "--worker-count",
                "1",
                "--sandbox-class",
                "gvisor",
            ]
        )

    @mock.patch("orchestrator.run")
    def test_actor_cpu(self, run):
        orchestrator.deploy_workloads(actor_cpu="250m")
        cmd = run.call_args.args[0]
        i = cmd.index("--actor-cpu")
        self.assertEqual(cmd[i + 1], "250m")

    @mock.patch("orchestrator.run")
    def test_worker_memory(self, run):
        orchestrator.deploy_workloads(worker_memory="12Gi")
        cmd = run.call_args.args[0]
        i = cmd.index("--worker-memory")
        self.assertEqual(cmd[i + 1], "12Gi")

    @mock.patch("orchestrator.run")
    def test_all_options(self, run):
        orchestrator.deploy_workloads(
            worker_count=3,
            sandbox_class="microvm",
            actor_memory="1536Mi",
            wait_timeout_secs=600,
            worker_memory="5Gi",
            actor_cpu="250m",
        )
        run.assert_called_once_with(
            [
                "benchmarking/workloads/deploy.sh",
                "--deploy",
                "--worker-count",
                "3",
                "--sandbox-class",
                "microvm",
                "--actor-cpu",
                "250m",
                "--actor-memory",
                "1536Mi",
                "--worker-memory",
                "5Gi",
                "--wait-timeout",
                "600",
            ]
        )


class ValidateAndNormalizeTest(unittest.TestCase):
    def _base_locust_test(self, **overrides):
        t = {
            "name": "glutton_cpu",
            "type": "locust",
            "targetCluster": "dev",
            "file": "/app/tests/glutton.py",
            "duration": "1m",
            "users": 1,
        }
        t.update(overrides)
        return t

    def test_cpu_cores_and_duty_cycle_normalized_into_flags(self):
        t = self._base_locust_test(
            actorCPU="250m",
            cpuCores=1,
            cpuDutyCycle=0.25,
            flags=["--min-wait-time", "1.0"],
        )
        orchestrator.validate_and_normalize_tests([t])
        self.assertEqual(
            t["flags"],
            [
                "--min-wait-time",
                "1.0",
                "--cpu-cores",
                "1",
                "--cpu-duty-cycle",
                "0.25",
            ],
        )

    def test_explicit_cpu_flags_not_duplicated(self):
        t = self._base_locust_test(
            cpuCores="1",
            cpuDutyCycle="0.1",
            flags=["--cpu-cores", "2", "--cpu-duty-cycle=0.5"],
        )
        orchestrator.validate_and_normalize_tests([t])
        self.assertEqual(t["flags"], ["--cpu-cores", "2", "--cpu-duty-cycle=0.5"])

    def test_invalid_actor_cpu_rejected(self):
        for bad in ("", "   ", True):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    orchestrator.validate_and_normalize_tests(
                        [self._base_locust_test(actorCPU=bad)]
                    )

    def test_invalid_cpu_cores_rejected(self):
        for bad in (-1, "1.5", "abc", True):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    orchestrator.validate_and_normalize_tests(
                        [self._base_locust_test(cpuCores=bad)]
                    )

    def test_invalid_cpu_duty_cycle_rejected(self):
        for bad in (-0.1, 1.5, "abc", False):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    orchestrator.validate_and_normalize_tests(
                        [self._base_locust_test(cpuDutyCycle=bad)]
                    )


class RunnerSizingTest(unittest.TestCase):
    TMPL = os.path.join(os.path.dirname(__file__), "manifests", "runner-job.yaml.tmpl")

    def render(self, test):
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d"}
        subs.update(locust.job_subs(test))
        text = orchestrator.render_template(self.TMPL, subs)
        job = next(d for d in yaml.safe_load_all(text) if d and d.get("kind") == "Job")
        return job["spec"]["template"]["spec"]["containers"][0]["resources"]

    def test_defaults(self):
        res = self.render({"file": "f", "duration": "1m", "users": 1})
        self.assertEqual(res, {"requests": {"cpu": "500m", "memory": "512Mi"}})

    def test_runner_cpu_and_memory(self):
        res = self.render(
            {"file": "f", "duration": "1m", "users": 1000, "runnerCpu": "4", "runnerMemory": "8Gi"}
        )
        self.assertEqual(res["requests"], {"cpu": "4", "memory": "8Gi"})
        self.assertNotIn("limits", res)

    def test_no_placeholder_survives(self):
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d"}
        subs.update(locust.job_subs({"file": "f", "duration": "1m", "users": 1}))
        self.assertNotIn("${", orchestrator.render_template(self.TMPL, subs))


class JobNameTest(unittest.TestCase):
    COMMIT = "ac41c06deadbeef"

    def test_short_name_keeps_full_test_name(self):
        name = orchestrator.job_name("Eng Review", self.COMMIT)
        self.assertRegex(name, r"^runner-eng-review-ac41c06-[0-9a-f]{6}$")

    def test_long_name_fits_a_label_and_keeps_suffix(self):
        test = "very-long-benchmark-name-that-overflows-the-kubernetes-label-limit"
        name = orchestrator.job_name(test, self.COMMIT)
        self.assertLessEqual(len(name), orchestrator.MAX_JOB_NAME_LEN)
        self.assertRegex(name, r"-ac41c06-[0-9a-f]{6}$")
        self.assertTrue(name.startswith("runner-very-long-benchmark-name"))

    def test_truncation_never_leaves_a_double_hyphen(self):
        # Cutting right after a hyphen must not yield "...-x--ac41c06-...".
        for i in range(1, 80):
            test = "-".join(["ab"] * i)
            name = orchestrator.job_name(test, self.COMMIT)
            self.assertLessEqual(len(name), orchestrator.MAX_JOB_NAME_LEN)
            self.assertNotIn("--", name, test)
            self.assertRegex(name, r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")

    def test_two_runs_of_the_same_long_test_differ(self):
        test = "x" * 100
        self.assertNotEqual(
            orchestrator.job_name(test, self.COMMIT),
            orchestrator.job_name(test, self.COMMIT),
        )


if __name__ == "__main__":
    unittest.main()
