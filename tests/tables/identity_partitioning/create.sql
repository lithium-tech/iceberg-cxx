-- Engine: Trino 471
CREATE SCHEMA IF NOT EXISTS warehouse.example_schema WITH (location = 's3a://warehouse/example');

CREATE TABLE warehouse.example_schema.identity_partitioning (
    col_bool BOOLEAN,
    col_int INTEGER,
    col_long BIGINT,
    col_float REAL,
    col_double DOUBLE,
    col_decimal DECIMAL(9, 2),
    col_date DATE,
    col_time TIME(6),
    col_timestamp TIMESTAMP(6),
    col_timestamptz TIMESTAMP(6) WITH TIME ZONE,
    col_string VARCHAR,
    col_uuid UUID,
    col_varbinary VARBINARY
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY ['col_bool', 'col_int', 'col_long', 'col_float', 'col_double', 'col_decimal', 'col_date', 'col_time', 'col_timestamp', 'col_timestamptz', 'col_string', 'col_uuid', 'col_varbinary'],
    location = 's3a://warehouse/identity_partitioning'
);

INSERT INTO
    warehouse.example_schema.identity_partitioning
VALUES
    (
        false,
        1,
        cast(2 AS BIGINT),
        cast(2.25 AS REAL),
        cast(2.375 AS DOUBLE),
        cast(3.0 AS DECIMAL(9, 2)),
        date '2025-03-03',
        cast('17:00:00.000000' AS TIME(6)),
        cast('2025-03-03 00:00:00' AS TIMESTAMP(6)),
        cast(
            '2025-03-03 00:00:00' AS TIMESTAMP(6) WITH TIME ZONE
        ),
        cast('some-string' AS VARCHAR),
        cast('12151fd2-7586-11e9-8f9e-2a86e4085a59' AS UUID),
        cast('some-varbinary' AS VARBINARY)
    );