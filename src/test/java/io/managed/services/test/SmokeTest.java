package io.managed.services.test;

import io.managed.services.test.framework.TestTag;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(TestTag.SMOKE)
public class SmokeTest extends TestBase {

    @Test
    void smokeTest() {
        assumeTrue(true);
    }
}
