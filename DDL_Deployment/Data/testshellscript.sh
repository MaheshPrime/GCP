bq query --project_id={project_id} --use_legacy_sql=false '
CREATE TABLE `<project>.<dataset>.placeholder_configuration_table`
(
Epic STRING NOT NULL,
source_entity STRING NOT NULL,
sources_attribute STRING NOT NULL,
last_modified_date STRING NOT NULL,
source_code STRING NOT NULL,
Key STRING NOT NULL,
Technical_key STRING NOT NULL,
Target_Table STRING NOT NULL,
target_table_abbreviation STRING NOT NULL,
Integration_id STRING NOT NULL,
target_ss_cd STRING,
target_cd STRING,
target_dw_uniq STRING,
target_dw STRING,
created_ts TIMESTAMP,
last_update_ts TIMESTAMP,
dev_type STRING,
partitioned_column STRING
)
OPTIONS(
labels=[("lm", "<local_mkt>"), ("env", "<env_var>")]
);
'