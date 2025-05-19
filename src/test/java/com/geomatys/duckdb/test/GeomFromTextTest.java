package com.geomatys.duckdb.test;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Isse: a valid SQL statement which works in DuckDB CLI produces an error when executed as a JDBC statement.
 * The test checks this behavior on both simple and prepared statements.
 * The query string used is the {@link #SQL_QUERY SQL_QUERY constant}.
 * <br/>
 * Command-line extract that works:
 *
 * <pre>
 * ❯ duckdb
 * v1.2.2 7c039464e4
 * Enter ".help" for usage hints.
 * Connected to a transient in-memory database.
 * Use ".open FILENAME" to reopen on a persistent database.
 * D INSTALL SPATIAL;
 * D LOAD SPATIAL;
 * D SELECT ST_GeomFromText('POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))') AS geom;
 * ┌──────────────────────────────────────────────────────────┐
 * │                           geom                           │
 * │                         geometry                         │
 * ├──────────────────────────────────────────────────────────┤
 * │ POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90)) │
 * └──────────────────────────────────────────────────────────┘
 * </pre>
 */
public class GeomFromTextTest extends AbstractDuckDbTest {

    private static final String SQL_QUERY = """
            SELECT ST_GeomFromText('POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))') AS geom
            """.trim();

    @Test
    public void testParseGeomFromTextFromPreparedStatement() {
        use(c -> {
            var rs = c.prepareStatement(SQL_QUERY).executeQuery();
            assertTrue(rs.next(), "A geometry should be returned, but result set is empty");
            assertNotNull(rs.getObject("geom"), "Returned geometry should not be null");
        });
    }

    @Test
    public void testParseGeomFromTextFromStatement() {
        use(c -> {
            var rs = c.createStatement().executeQuery(SQL_QUERY);
            assertTrue(rs.next(), "A geometry should be returned, but result set is empty");
            assertNotNull(rs.getObject("geom"), "Returned geometry should not be null");
        });
    }
}
