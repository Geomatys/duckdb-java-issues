package com.geomatys.duckdb.test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verify that two unrelated result-set do not interfer with each other.
 *
 * @see #resultSetShouldEndProperlyEvenWhenAnotherResultSetHasBeenClosedMeanwhile() Most important/detailed test that shows that closing a resultset can corrupt end of stream of another result set.
 */
public class NestedResultSetTest extends AbstractDuckDbTest {

    /**
     * Return the number of elements in a table.
     */
    private static long count(String tableName, Connection c) throws SQLException {
        try (var stmt = c.prepareStatement("select count(*) from "+tableName);
             var r = stmt.executeQuery()
        ) {
            assertTrue(r.next(), "Count should return exactly one row");
            return r.getLong(1);
        }
    }

    @Test
    public void resultSetShouldEndProperlyEvenWhenAnotherResultSetHasBeenClosedMeanwhile() {
        use(connection -> {
            final long nbAuthors = count("\"author\"", connection);
            try (var authorStatement = connection.createStatement();
                 var authors = authorStatement.executeQuery("SELECT * FROM \"author\"")
            ) {

                // Start consuming first result-set.
                // The important point is we do not want to reach end of result yet.
                assertTrue(authors.next(), "Query should return at least on author");

                // Open, consume and close another statement/result-set
                // (to another table, to ensure it is not related in any way to the first statement)
                try (var bookStatement = connection.createStatement();
                     var books = bookStatement.executeQuery("SELECT * FROM \"book\"")
                ) {
                    assertTrue(books.next(), "At least one book should exist");
                }

                // Consume remaining rows
                for (int i = 0; i < nbAuthors - 1; i++) {
                    assertTrue(authors.next(), "There should still be available authors");
                }

                // Expect result-set to properly notify end of data without raising error
                try {
                    assertFalse(authors.next());
                } catch (Exception e) {
                    fail("ResultSet should notify end of stream without issuing errors", e);
                }
            }
        });
    }

    @Test
    public void openingOrClosingAResultSetShouldNotCloseExistingOnes() {
        use(connection -> {
            try (var authors = connection.createStatement().executeQuery("SELECT * FROM \"author\"")) {
                assertTrue(authors.next(), "Query should return authors");
                try (var books = connection.createStatement().executeQuery("SELECT * FROM \"book\"")) {
                    assertTrue(books.next(), "Query should return books");
                    assertFalse(authors.isClosed(), "Previous result set should not be closed");
                    assertTrue(authors.next(), "There should be more authors");
                }
                assertFalse(authors.isClosed(), "Previous result set should not be closed");
            }
        });
    }

    @Test
    public void closingAStatementShouldNotImpactOtherStatements() {
        use(connection -> {
            try (var authorStatement = connection.createStatement();
                 var authors = authorStatement.executeQuery("SELECT * FROM \"author\"")
            ) {
                assertTrue(authors.next(), "Query should return authors");
                try (var bookStatement = connection.createStatement();
                     var books = bookStatement.executeQuery("SELECT * FROM \"book\"")
                ) {
                    assertTrue(books.next(), "Query should return books");
                    assertFalse(authorStatement.isClosed(), "Previous statement should not be closed");
                    assertFalse(authors.isClosed(), "Previous result set should not be closed");
                    assertTrue(authors.next(), "There should be more authors");
                }
                assertFalse(authors.isClosed(), "Previous result set should not be closed");
                assertFalse(authorStatement.isClosed(), "Previous statement should not be closed");
            }
        });
    }

    @Test
    public void closingAPreparedStatementShouldNotImpactOtherStatements() {
        use(connection -> {
            try (var authorStatement = connection.prepareStatement("SELECT * FROM \"author\"");
                 var authors = authorStatement.executeQuery()
            ) {
                assertTrue(authors.next(), "Query should return authors");
                try (var bookStatement = connection.prepareStatement("SELECT * FROM \"book\" WHERE \"author\" = ?")) {
                    bookStatement.setString(1, authors.getString(1));
                    try (var books = bookStatement.executeQuery()) {
                        assertTrue(books.next(), "Query should return books");
                        assertFalse(authorStatement.isClosed(), "Previous statement should not be closed");
                        assertFalse(authors.isClosed(), "Previous result set should not be closed");
                        assertTrue(authors.next(), "There should be more authors");
                    }
                }
                assertFalse(authors.isClosed(), "Previous result set should not be closed");
                assertFalse(authorStatement.isClosed(), "Previous statement should not be closed");
            }
        });
    }

    @Test
    public void verifyInnerResultSetNotClosed() {
        use(c -> verifyInnerResultSetNotClosed(c));
    }


    @Test
    public void verifyMetadataNestedCallsAreAllowed() throws SQLException {
        use(c -> verifyMetadataNestedCallsAreAllowed(c));
    }

    private void verifyInnerResultSetNotClosed(Connection c) throws SQLException {
        record Book(String title, String author) {}
        var result = new ArrayList<Book>();
        try (var authorStatement = c.prepareStatement("SELECT name FROM \"author\"");
             var bookStatement = c.prepareStatement("SELECT * FROM \"book\" WHERE \"author\" = ?");
             var authors = authorStatement.executeQuery()
        ) {
            assertFalse(authorStatement.isCloseOnCompletion(), "By default, statement should not close on completion");
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

    private void verifyMetadataNestedCallsAreAllowed(Connection c) throws SQLException {
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
