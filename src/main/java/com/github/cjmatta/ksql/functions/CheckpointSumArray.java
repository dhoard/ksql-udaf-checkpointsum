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
                return 0d;
            }

            public Double aggregate(final List<String> list, final Double aggregateValue) {
                if (null == list) {
                    throw new RuntimeException("Parameter array is null");
                }

                if (2 != list.size()) {
                    throw new RuntimeException("Parameter array requires 2 parameters, parameter count = [" + list.size() + "]");
                }

                String type = list.get(0);
                double value = 0d;

                try {
                    value = Double.valueOf(list.get(1));
                } catch (Throwable t) {
                    throw new RuntimeException("VALUE parameter can't be converted to a double");
                }

                if (DELTA.equalsIgnoreCase(type) || DELTA_SHORT.equalsIgnoreCase(type)) {
                    return aggregateValue + value;
                } else if (ABSOLUTE.equalsIgnoreCase(type) || ABSOLUTE_SHORT.equalsIgnoreCase(type)) {
                    return value;
                } else {
                    throw new RuntimeException("Invalid TYPE = [" + type + "]");
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
}