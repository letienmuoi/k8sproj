package vn.muoilt.pipeline;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public final class MedallionPipelineJob {
    private MedallionPipelineJob() {
    }

    public static void main(String[] args) throws Exception {
        String kafkaBootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String sourceTopic = env("SOURCE_TOPIC", "raw-data");
        String sinkTopic = env("SINK_TOPIC", "refined-data");
        String icebergRestUri = env("ICEBERG_REST_URI", "http://iceberg-rest:8181");
        String warehouse = env("ICEBERG_WAREHOUSE", "s3://iceberg-warehouse");
        String s3Endpoint = env("S3_ENDPOINT", "http://minio:9000");
        String s3AccessKey = env("S3_ACCESS_KEY", "minioadmin");
        String s3SecretKey = env("S3_SECRET_KEY", "minioadmin123");

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        executionEnvironment.enableCheckpointing(Duration.ofSeconds(15).toMillis(), CheckpointingMode.EXACTLY_ONCE);
        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(Duration.ofSeconds(5).toMillis());

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment, settings);
        tableEnvironment.createTemporarySystemFunction("normalize_line", NormalizeLineFunction.class);

        tableEnvironment.executeSql("""
                CREATE TABLE raw_lines (
                    line STRING,
                    ingest_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = '%s',
                    'properties.bootstrap.servers' = '%s',
                    'properties.group.id' = 'ai-data-pipeline-v1',
                    'scan.startup.mode' = 'earliest-offset',
                    'format' = 'raw',
                    'raw.charset' = 'UTF-8'
                )
                """.formatted(sql(sourceTopic), sql(kafkaBootstrapServers)));

        tableEnvironment.executeSql("""
                CREATE TABLE refined_lines (
                    original_line STRING,
                    normalized_line STRING,
                    character_count INT,
                    processed_at TIMESTAMP_LTZ(3)
                ) WITH (
                    'connector' = 'kafka',
                    'topic' = '%s',
                    'properties.bootstrap.servers' = '%s',
                    'format' = 'json',
                    'json.timestamp-format.standard' = 'ISO-8601'
                )
                """.formatted(sql(sinkTopic), sql(kafkaBootstrapServers)));

        tableEnvironment.executeSql("""
                CREATE CATALOG lakehouse WITH (
                    'type' = 'iceberg',
                    'catalog-impl' = 'org.apache.iceberg.rest.RESTCatalog',
                    'uri' = '%s',
                    'warehouse' = '%s',
                    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
                    's3.endpoint' = '%s',
                    's3.access-key-id' = '%s',
                    's3.secret-access-key' = '%s',
                    's3.path-style-access' = 'true',
                    'client.region' = 'us-east-1'
                )
                """.formatted(
                sql(icebergRestUri),
                sql(warehouse),
                sql(s3Endpoint),
                sql(s3AccessKey),
                sql(s3SecretKey)));

        tableEnvironment.executeSql("CREATE DATABASE IF NOT EXISTS lakehouse.medallion");
        tableEnvironment.executeSql("""
                CREATE TABLE IF NOT EXISTS lakehouse.medallion.bronze_lines (
                    line STRING,
                    ingest_time TIMESTAMP_LTZ(3),
                    stored_at TIMESTAMP_LTZ(3)
                )
                """);
        tableEnvironment.executeSql("""
                CREATE TABLE IF NOT EXISTS lakehouse.medallion.silver_lines (
                    original_line STRING,
                    normalized_line STRING,
                    character_count INT,
                    processed_at TIMESTAMP_LTZ(3)
                )
                """);
        tableEnvironment.executeSql("""
                CREATE TABLE IF NOT EXISTS lakehouse.medallion.gold_ai_ready (
                    normalized_line STRING,
                    character_count INT,
                    processed_at TIMESTAMP_LTZ(3)
                )
                """);

        String validRows = "line IS NOT NULL AND CHAR_LENGTH(TRIM(line)) > 0";
        StatementSet statementSet = tableEnvironment.createStatementSet();
        statementSet.addInsertSql("""
                INSERT INTO lakehouse.medallion.bronze_lines
                SELECT line, ingest_time, CURRENT_TIMESTAMP
                FROM raw_lines
                """);
        statementSet.addInsertSql("""
                INSERT INTO lakehouse.medallion.silver_lines
                SELECT line, normalize_line(line), CHAR_LENGTH(normalize_line(line)), CURRENT_TIMESTAMP
                FROM raw_lines
                WHERE %s
                """.formatted(validRows));
        statementSet.addInsertSql("""
                INSERT INTO refined_lines
                SELECT line, normalize_line(line), CHAR_LENGTH(normalize_line(line)), CURRENT_TIMESTAMP
                FROM raw_lines
                WHERE %s
                """.formatted(validRows));
        statementSet.addInsertSql("""
                INSERT INTO lakehouse.medallion.gold_ai_ready
                SELECT normalize_line(line), CHAR_LENGTH(normalize_line(line)), CURRENT_TIMESTAMP
                FROM raw_lines
                WHERE %s
                """.formatted(validRows));

        TableResult result = statementSet.execute();
        result.await();
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String sql(String value) {
        return value.replace("'", "''");
    }
}
