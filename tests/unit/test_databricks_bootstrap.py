import pytest

from scripts.bootstrap_databricks import build_bootstrap_statements, execute_bootstrap


class FakeSpark:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def sql(self, statement: str) -> None:
        self.statements.append(statement)


def test_bootstrap_ddl_is_idempotent_and_qualified() -> None:
    spark = FakeSpark()

    statements = execute_bootstrap(spark, "novaretail_dev")

    assert statements[0] == "CREATE CATALOG IF NOT EXISTS `novaretail_dev`"
    assert "CREATE SCHEMA IF NOT EXISTS `novaretail_dev`.`gold`" in statements
    assert statements[-1] == ("CREATE VOLUME IF NOT EXISTS `novaretail_dev`.`platform`.`data`")
    assert spark.statements == list(statements)


@pytest.mark.parametrize("catalog", ["bad-name", "catalog; DROP SCHEMA x", "", "two words"])
def test_bootstrap_rejects_identifier_injection(catalog: str) -> None:
    with pytest.raises(ValueError, match="Unsafe Unity Catalog identifier"):
        build_bootstrap_statements(catalog)
