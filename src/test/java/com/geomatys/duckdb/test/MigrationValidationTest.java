package com.geomatys.duckdb.test;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verify that test database setup works properly. No error expected here.
 */
public class MigrationValidationTest extends AbstractDuckDbTest {

    @Test
    public void verifyMigration() {
        use(c -> {
            var result = c.createStatement().executeQuery("SELECT * from \"author\"");
            Assertions.assertTrue(result.next(), "Result shouldn't be empty");
        });
    }
}
