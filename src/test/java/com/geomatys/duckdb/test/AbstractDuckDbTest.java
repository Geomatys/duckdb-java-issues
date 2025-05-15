package com.geomatys.duckdb.test;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.duckdb.DuckDBDriver;
import org.flywaydb.core.Flyway;

public class AbstractDuckDbTest {

    private void setup(String jdbcUrl) {
        Flyway.configure()
              .validateOnMigrate(true)
              .dataSource(jdbcUrl, null, null)
              .load()
              .migrate();
    }

    final protected void use(ConnectionConsumer action) {
        use(true, true, action);
    }

    final protected void use(boolean readOnly, boolean streamResults, ConnectionConsumer action) {
        try {
            var file = Files.createTempFile("test", ".duckdb");
            Files.delete(file);
            try (Closeable deleteFile = () -> Files.deleteIfExists(file)) {
                final var jdbcUrl = "jdbc:duckdb:file://" + file.toAbsolutePath();
                setup(jdbcUrl);
                Properties props = new Properties();
                props.setProperty(DuckDBDriver.DUCKDB_READONLY_PROPERTY, Boolean.toString(readOnly));
                props.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, Boolean.toString(streamResults));
                try (var c = DriverManager.getConnection(jdbcUrl, props)) {
                    try (var stmt = c.createStatement()) { stmt.execute("LOAD SPATIAL"); }
                    action.accept(c);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot use temporary files", e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    protected interface ConnectionConsumer {
        void accept(Connection connection) throws SQLException;
    }
}
