import json
import os

import pytest
from dbt.tests.util import run_dbt

ref_models__table_comment_sql = """
{{
  config(
    materialized = "table",
    persist_docs = {"relation": true, "columns": true},
  )
}}

select
    'foo' as first_name,
    'bar' as second_name

"""

ref_models__replicated_table_comment_sql = """
{{
  config(
    materialized = "table",
    persist_docs = {"relation": true, "columns": true},
    engine="ReplicatedMergeTree('/clickhouse/tables/{uuid}/one-shard', '{replica}' )"
  )
}}

select
    'foo' as first_name,
    'bar' as second_name

"""

ref_models__view_comment_sql = """
{{
  config(
    materialized = "view",
    persist_docs = {"relation": true, "columns": true},
  )
}}

select
    'foo' as first_name,
    'bar' as second_name

"""

ref_models__schema_yml = """
version: 2

models:
  - name: table_comment
    description: "YYY table"
    columns:
      - name: first_name
        description: "XXX first description"
      - name: second_name
        description: "XXX second description"
  - name: replicated_table_comment
    description: "YYY table"
    columns:
      - name: first_name
        description: "XXX first description"
      - name: second_name
        description: "XXX second description"
  - name: view_comment
    description: "YYY view"
    columns:
      - name: first_name
        description: "XXX first description"
      - name: second_name
        description: "XXX second description"
"""


class TestBaseComment:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": ref_models__schema_yml,
            "table_comment.sql": ref_models__table_comment_sql,
            "replicated_table_comment.sql": ref_models__replicated_table_comment_sql,
            "view_comment.sql": ref_models__view_comment_sql,
        }

    @pytest.mark.parametrize(
        'model_name',
        ["table_comment", "replicated_table_comment", "view_comment"],
    )
    def test_comment(self, project, model_name):
        if os.environ.get('DBT_CH_TEST_CLOUD', '').lower() in ('1', 'true', 'yes'):
            pytest.skip('Not running comment test for cloud')
        run_dbt(["run"])
        run_dbt(["docs", "generate"])
        with open("target/catalog.json") as fp:
            catalog_data = json.load(fp)

        assert "nodes" in catalog_data
        column_node = catalog_data["nodes"][f"model.test.{model_name}"]
        for column in column_node["columns"].keys():
            column_comment = column_node["columns"][column]["comment"]
            assert column_comment.startswith("XXX")

        assert column_node['metadata']['comment'].startswith("YYY")

        # Ensure comment is propoagated to all replicas on cluster
        #cluster = project.test_config['cluster']

#         local_relation = relation_from_name(project.adapter, model_name)
#         if cluster:
#             ensure_column_comments_consistent_across_replicas(project, cluster, local_relation)
#             ensure_table_comment_on_cluster(project, cluster, local_relation)



# def ensure_table_comment_on_cluster(project, cluster, local_relation):
#     """Ensure all replicas have same comment for given relation"""
#     # Returns 'ok' if exactly one distinct comment exists across all replicas for this table; otherwise 'mismatch'.
#     sql = f"""
#     SELECT
#         if(COUNT(DISTINCT comment) = 1, 'ok', 'mismatch') AS status
#     FROM clusterAllReplicas('{cluster}', system.tables)
#     WHERE database = currentDatabase()
#       AND name = '{local_relation.identifier}'
#     """
#     result = project.run_sql(sql, fetch="one")
#     assert result[0] == "ok"

#     sql = f"""
#     SELECT
#       hostname(),
#       comment
#     FROM clusterAllReplicas('{cluster}', system.tables)
#     WHERE `table` = '{local_relation.identifier}'
#     """

#     result = project.run_sql(sql, fetch="all")

#     for _table_row in result:
#         assert _table_row[-1].startswith("YYY")

# def ensure_column_comments_consistent_across_replicas(project, cluster, local_relation):
#     # This query groups by column name and checks that each has exactly one distinct comment across replicas.
#     check_sql = f"""
#         SELECT
#             name AS column_name,
#             COUNT(DISTINCT comment) AS distinct_comment_count,
#             groupArray((hostName(), comment)) AS per_replica_comments
#         FROM clusterAllReplicas('{cluster}', system.columns)
#         WHERE database = currentDatabase()
#           AND table = '{local_relation.identifier}'
#         GROUP BY column_name
#     """
#     rows = project.run_sql(check_sql, fetch="all")

#     mismatches = [r for r in rows if r[1] != 1]
#     if mismatches:
#         print("Column comment mismatches:", mismatches)

#     assert not mismatches

#     sql = f"""
#     SELECT
#       name,
#       groupArray(hostname()) as hosts,
#       groupUniqArray(comment) as comments,
#       length(comments) as num_comments
#     FROM clusterAllReplicas('{cluster}', system.columns)
#     WHERE table = '{local_relation.identifier}'
#     GROUP BY name
#     """

#     result = project.run_sql(sql, fetch="all")

#     print("WOW"*100)
#     print("\n\n")
#     print(result)
#     print("WOW"*100)

#     for _col in result:
#         assert _col[-1] == 1

#     assert result == []


#     sql = f"""
#     SELECT
#       name,
#       count(hostname())
#     FROM clusterAllReplicas('{cluster}', system.columns)
#     WHERE table = '{local_relation.identifier}'
#     GROUP BY name
#     """
#     result = project.run_sql(sql, fetch="all")
#     assert result[-1] == NUM_CLUSTER_NODES

#     assert result == []



