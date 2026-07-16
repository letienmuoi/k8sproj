package vn.muoilt.pipeline;

import org.apache.flink.table.functions.ScalarFunction;

public final class NormalizeLineFunction extends ScalarFunction {
    public String eval(String value) {
        return LineNormalizer.normalize(value);
    }
}
