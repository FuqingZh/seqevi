from scripts.ci_change_scope import ChangeScope, classify_changes


def test_lightweight_documentation_uses_documentation_gate_only() -> None:
    assert classify_changes(
        [
            "docs/README.md",
            "docs/archive/testing/old.md",
            "docs/benchmarks/result.md",
            "docs/how-to-guides/first.md",
            "docs/implementation-plan/plan.md",
        ]
    ) == ChangeScope(tests=False, distribution=False, dbcan=False)


def test_contract_document_runs_tests_without_image_builds() -> None:
    for path in ("docs/architecture/contract.md", "AGENTS.md"):
        assert classify_changes([path]) == ChangeScope(
            tests=True,
            distribution=False,
            dbcan=False,
        )


def test_test_change_runs_only_test_matrix() -> None:
    assert classify_changes(["tests/test_store.py"]) == ChangeScope(
        tests=True,
        distribution=False,
        dbcan=False,
    )


def test_source_and_package_inputs_run_all_ci_jobs() -> None:
    for path in (
        "src/seqevi/cli.py",
        "pyproject.toml",
        "README.md",
        "pdm.lock",
        "scripts/ci_change_scope.py",
        ".github/workflows/ci.yml",
    ):
        assert classify_changes([path]) == ChangeScope(
            tests=True,
            distribution=True,
            dbcan=True,
        )


def test_each_image_boundary_runs_its_own_build() -> None:
    assert classify_changes(["Dockerfile"]) == ChangeScope(
        tests=True,
        distribution=True,
        dbcan=False,
    )
    assert classify_changes(["containers/dbcan/Dockerfile"]) == ChangeScope(
        tests=True,
        distribution=False,
        dbcan=True,
    )


def test_mixed_or_unknown_change_fails_closed() -> None:
    assert classify_changes(
        ["docs/how-to-guides/first.md", "src/seqevi/cli.py"]
    ) == ChangeScope(tests=True, distribution=True, dbcan=True)
    assert classify_changes(["unknown-config.ini"]) == ChangeScope(
        tests=True,
        distribution=True,
        dbcan=True,
    )
    assert classify_changes([]) == ChangeScope(
        tests=True,
        distribution=True,
        dbcan=True,
    )
