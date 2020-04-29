package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "checkpoint_sum", description = "CheckpointSum UDAF.")
public class CheckpointSum  {

    public static final String TYPE = "TYPE";
    public static final String VALUE = "VALUE";

    public static final String DELTA = "DELTA";
    public static final String DELTA_SHORT = "D";

    public static final String ABSOLUTE = "ABSOLUTE";
    public static final String ABSOLUTE_SHORT = "A";

    @UdafFactory(description = "CheckpointSum UDAF", paramSchema = "STRUCT<TYPE VARCHAR, VALUE DOUBLE>")
    public static Udaf<Struct, Double, Double> getUDAF() {

        return new Udaf<Struct, Double, Double>() {

            public Double initialize() {
                return 0.0;
            }

            public Double aggregate(final Struct struct, final Double aDouble) {
                Object object = struct.get(TYPE);

                if (null == object) {
                    throw new RuntimeException("object is null");
                }

                String type = object.toString();

                if (DELTA.equalsIgnoreCase(type) || DELTA_SHORT.equalsIgnoreCase(type)) {
                    return aDouble + (Double) struct.get(VALUE);
                } else if (ABSOLUTE.equalsIgnoreCase(type) || ABSOLUTE_SHORT.equalsIgnoreCase(type)) {
                    return (Double) struct.get(VALUE);
                } else {
                    throw new RuntimeException("Invalid type, type = [" + type + "]");
                }
            }

            public Double merge(final Double aDouble, final Double a1) {
                throw new RuntimeException("merge is not supported");
            }

            public Double map(final Double aDouble) {
                return aDouble;
            }
        };
    }
}