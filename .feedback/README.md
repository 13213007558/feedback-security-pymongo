# Feedback task starting environment

Upstream: https://github.com/mongodb/mongo-python-driver
Upstream commit: `95ca54931cbb52307c45f3824256f5c3efb8b2aa`
Prepared on macOS with CPython 3.12.11. No external database, queue, or cloud account is required.
The dependency file pins the installed runtime and test packages for the selected baseline. It is not a claim that all optional upstream test suites are installed.

From the repository root:

```sh
export NO_INTERNET=1 TZ=UTC UV_NO_CONFIG=1

uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r .feedback/requirements.txt -e .
.venv/bin/python -m unittest test.test_bson test.test_saslprep test.test_raw_bson
```

Scope note: The scope is the command below, not all optional upstream integrations.

Observed baseline (2026-11-11): `OK (skipped=7)`
Whole-command wall time: 11.659 seconds, exit code 0. This is one local measurement, not a runtime guarantee. The first install is excluded.

This seed adds preparation metadata only. Application sources and upstream tests are unchanged; there is no task solution in this commit.
