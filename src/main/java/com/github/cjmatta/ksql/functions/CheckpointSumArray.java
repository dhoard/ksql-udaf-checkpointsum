package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;

@UdafDescription(name = "checkpoint_sum_array", description = "CheckpointSumArray UDAF.")
public class CheckpointSumArray {

    public static final String TYPE = "TYPE";
    public static final String VALUE = "VALUE";

    public static final String DELTA = "DELTA";
    public static final String DELTA_SHORT = "D";

    public static final String ABSOLUTE = "ABSOLUTE";
    public static final String ABSOLUTE_SHORT = "A";

    @UdafFactory(description = "CheckpointSumArray UDAF", paramSchema = "ARRAY<VARCHAR>")
    public static Udaf<List<String>, Double, Double> getUDAF() {

        return new Udaf<List<String>, Double, Double>() {

            public Double initialize() {
                return 0d;
            }

            public Double aggregate(final List<String> list, Double aggregateValue) {
                if (null == list) {
                    printException("Parameter array is null");
                    return null;
                }

                if (2 != list.size()) {
                    printException("Parameter array requires 2 parameters, parameter count = [" + list.size() + "]");
                    return null;
                }

                String type = list.get(0);
                double value = 0d;

                try {
                    value = Double.valueOf(list.get(1));
                } catch (Throwable t) {
                    printException("VALUE parameter can't be converted to a double");
                    return null;
                }

                if (null == aggregateValue) {
                    System.err.println("null aggregate value, reset to default");
                    aggregateValue = initialize();
                }

                if (DELTA.equalsIgnoreCase(type) || DELTA_SHORT.equalsIgnoreCase(type)) {
                    return aggregateValue + value;
                } else if (ABSOLUTE.equalsIgnoreCase(type) || ABSOLUTE_SHORT.equalsIgnoreCase(type)) {
                    return value;
                } else {
                    printException("Invalid TYPE = [" + type + "]");
                    return null;
                }
            }

            public Double merge(final Double aDouble, final Double a1) {
                throw new RuntimeException("Merge is not supported");
            }

            public Double map(final Double aDouble) {
                return aDouble;
            }
        };
    }

    private static void printException(String message) {
        RuntimeException runtimeException = new RuntimeException(message);
        runtimeException.printStackTrace();
    }
}