package org.example;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.io.File;
import java.util.List;

/**
 * Upserting to Iceberg V2 Table with FlinkSink.
 */
public class FlinkSinkUpsert {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        final String catalogPath = "file://" + new File("catalog/").getAbsolutePath();
        tEnv.executeSql(
                "CREATE CATALOG catalog WITH (" +
                        "'type'='iceberg', " +
                        "'catalog-type'='hadoop', " +
                        "'warehouse'='" + catalogPath + "', " +
                        "'property-version'='1' " +
                        ");"
        );
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS catalog.db;");
        tEnv.executeSql("DROP TABLE IF EXISTS catalog.db.flink_sink_upsert");
        tEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS catalog.db.flink_sink_upsert (id int primary key, some_value string) " +
                        "WITH ('format-version'='2')"
        );

        final DataStreamSource<Row> source = env.fromCollection(
                List.of(
                        Row.of(1, "value 1"),
                        Row.of(1, "value 2")
                )
        );
        final TableSchema.Builder tableSchemaBuilder = TableSchema.builder();
        tableSchemaBuilder.field("id", new AtomicDataType(new IntType(false)));
        tableSchemaBuilder.field("some_value", new AtomicDataType(new VarCharType()));

        FlinkSink.forRow(source, tableSchemaBuilder.build())
                .tableLoader(TableLoader.fromHadoopTable(catalogPath + "/db/flink_sink_upsert"))
                .upsert(true)
                .append();

        env.execute();

        System.out.println("select count(*)");
        try (CloseableIterator<Row> result = tEnv.executeSql("SELECT count(*) FROM catalog.db.flink_sink_upsert").collect()) {
            // expected output: 1
            result.forEachRemaining(System.out::println);
        }

        System.out.println("select *");
        try (CloseableIterator<Row> result = tEnv.executeSql("SELECT * FROM catalog.db.flink_sink_upsert").collect()) {
            // expected output:
            // 1, value 2
            result.forEachRemaining(System.out::println);
        }
    }
}
