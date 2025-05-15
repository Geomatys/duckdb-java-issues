package com.geomatys.duckdb.test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Try to reproduce an error that sometimes unexpectedly close all opened result sets.
 */
public class NestedResultSetTest extends AbstractDuckDbTest {

    @Test
    public void verifyInnerResultSetNotClosed() {
        use(c -> {
            verifyMetadataInnerResultSetNotClosed(c);
            verifyInnerResultSetNotClosed(c);
        });
    }

    private void verifyInnerResultSetNotClosed(Connection c) throws SQLException {
        record Book(String title, String author) {}
        var result = new ArrayList<Book>();
        try (var authorStatement = c.prepareStatement("SELECT name FROM \"author\"");
             var bookStatement = c.prepareStatement("SELECT * FROM \"book\" WHERE \"author\" = ?");
             ResultSet authors = authorStatement.executeQuery()) {
            while (authors.next()) {
                bookStatement.setString(1, authors.getString(1));
                try (ResultSet books = bookStatement.executeQuery()) {
                    while (books.next()) {
                        result.add(new Book(books.getString(1), books.getString(2)));
                    }
                }
            }
        }

        assertFalse(result.isEmpty(), "Book list shouldn't be empty");
    }

    private void verifyMetadataInnerResultSetNotClosed(Connection c) throws SQLException {
        DatabaseMetaData metadata = c.getMetaData();
        var cnames = new ArrayList<>();
        try (ResultSet tables = metadata.getTables(null, null, null, null)) {
            while (tables.next()) {
                final var tableName = tables.getString(3);
                try (ResultSet columns = metadata.getColumns(null, null, tableName, null)) {
                    while (columns.next()) {
                        cnames.add(tableName + ": " + columns.getString(4));
                    }
                }
            }
        }

        assertFalse(cnames.isEmpty(), "Column list should not be empty");
    }
}
