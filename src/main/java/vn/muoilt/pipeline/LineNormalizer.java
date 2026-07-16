package vn.muoilt.pipeline;

import java.text.Normalizer;
import java.util.Locale;

public final class LineNormalizer {
    private LineNormalizer() {
    }

    public static String normalize(String value) {
        if (value == null) {
            return null;
        }

        return Normalizer.normalize(value, Normalizer.Form.NFKC)
                .trim()
                .replaceAll("\\s+", " ")
                .toLowerCase(Locale.ROOT);
    }
}
