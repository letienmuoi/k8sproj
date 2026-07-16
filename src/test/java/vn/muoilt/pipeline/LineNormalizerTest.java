package vn.muoilt.pipeline;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class LineNormalizerTest {
    @Test
    void normalizesCaseWhitespaceAndUnicodeWidth() {
        assertEquals("du lieu ai", LineNormalizer.normalize("  DU   LIEU\tAI  "));
        assertEquals("abc 123", LineNormalizer.normalize("ＡＢＣ １２３"));
    }

    @Test
    void preservesNull() {
        assertNull(LineNormalizer.normalize(null));
    }
}
