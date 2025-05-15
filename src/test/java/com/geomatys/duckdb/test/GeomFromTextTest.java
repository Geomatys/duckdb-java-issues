package com.geomatys.duckdb.test;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
