package com.geomatys.duckdb.test;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Consumer;
import org.duckdb.DuckDBDriver;
import org.flywaydb.core.Flyway;

/**
 * Provides a utility {@link #use(ConnectionConsumer)} function and run code against an ephemeral test database.
 *
 * @see #use(boolean, boolean, ConnectionConsumer)
 */
public class AbstractDuckDbTest {

    private void setup(String jdbcUrl) {
        Flyway.configure()
              .validateOnMigrate(true)
              .dataSource(jdbcUrl, null, null)
              .load()
              .migrate();
    }

    /**
     * Setup and use an ephemeral file-based Duckdb database.
     *
     * @param action User action that receive a {@link Connection } to the database as input.
     *               Connection is automatically closed after user action is called.
     *               User action is called exactly once after database setup.
     */
    final protected void use(ConnectionConsumer action) {
        use(true, true, action);
    }

    /**
     * Setup and use an ephemeral file-based Duckdb database.
     *
     * @param readOnly If true, opens a read-only connection to the database for user action.
     * @param streamResults If true, enables {@link DuckDBDriver#JDBC_STREAM_RESULTS result data streaming}.
     * @param action User action that receive a {@link Connection } to the database as input.
     *               Connection is automatically closed after user action is called.
     *               User action is called exactly once after database setup.
     */
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

    /**
     * A specialized {@link Consumer} that receives an SQL connection as input and can raise {@link SQLException SQL specific exceptions.}.
     */
    @FunctionalInterface
    protected interface ConnectionConsumer {
        void accept(Connection connection) throws SQLException;
    }
}
