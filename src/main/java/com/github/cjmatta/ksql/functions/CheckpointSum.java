package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;
import org.apache.kafka.connect.data.Field;
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
                return 0d;
            }

            public Double aggregate(final Struct struct, final Double aggregateValue) {
                Object typeObject = struct.get(TYPE);
                Object valueObject = struct.get(VALUE);

                if (null == typeObject) {
                    throw new RuntimeException("TYPE is null");
                }

                if (null == valueObject) {
                    throw new RuntimeException("VALUE is null");
                }

                String type = typeObject.toString();
                double value = 0d;

                try {
                    value = struct.getFloat64(VALUE);
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

            public Double merge(final Double value1, final Double value2) {
                throw new RuntimeException("Merge is not supported");
            }

            public Double map(final Double value) {
                return value;
            }
        };
    }
}