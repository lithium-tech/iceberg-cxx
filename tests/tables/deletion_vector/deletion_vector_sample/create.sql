-- Engine: Spark 3.5.2

CREATE DATABASE IF NOT EXISTS my_catalog

CREATE TABLE my_catalog.deletion_vector.deletion_vector_sample (
        id BIGINT,
        name STRING,
        ts TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES (
        'format-version'='3',
        'write.delete.mode'='merge-on-read',
        'write.update.mode'='merge-on-read'
    );

INSERT INTO my_catalog.deletion_vector.deletion_vector_sample
SELECT
  id,
  CONCAT('Name_', CAST(id AS STRING)) AS name,
  CURRENT_TIMESTAMP() AS ts
FROM (
  SELECT explode(sequence(1, 10)) AS id
);

DELETE FROM my_catalog.deletion_vector.deletion_vector_sample WHERE id % 2 = 0;
