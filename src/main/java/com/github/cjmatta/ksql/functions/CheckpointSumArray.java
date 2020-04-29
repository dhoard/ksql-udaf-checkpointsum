package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import org.apache.kafka.connect.data.Struct;

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
                return 0.0;
            }

            public Double aggregate(final List<String> list, final Double aDouble) {
                if (null == list) {
                    System.err.println("parameter array is null");
                    return null;
                }

                if (2 != list.size()) {
                    System.err.println("parameter array requires 2 parameters, parameter count = [" + list.size() + "]");
                }

                String type = list.get(0);
                Double value = 0d;

                try {
                    value = Double.valueOf(list.get(1));
                } catch (Throwable t) {
                    System.err.println("value parameter can't be cast to a Double");
                    return null;
                }

                if (DELTA.equalsIgnoreCase(type) || DELTA_SHORT.equalsIgnoreCase(type)) {
                    return aDouble + value;
                } else if (ABSOLUTE.equalsIgnoreCase(type) || ABSOLUTE_SHORT.equalsIgnoreCase(type)) {
                    return value;
                } else {
                    System.err.println("Invalid type, type = [" + type + "]");
                    return null;
                }
            }

            public Double merge(final Double aDouble, final Double a1) {
                return null;
            }

            public Double map(final Double aDouble) {
                return aDouble;
            }
        };
    }
}