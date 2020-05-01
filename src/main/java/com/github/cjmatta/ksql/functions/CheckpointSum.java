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
                return 0d;
            }

            public Double aggregate(final Struct struct, Double aggregateValue) {
                Object typeObject = struct.get(TYPE);
                Object valueObject = struct.get(VALUE);

                if (null == typeObject) {
                    printException("TYPE is null");
                    return null;
                }

                if (null == valueObject) {
                    printException("VALUE is null");
                    return null;
                }

                String type = typeObject.toString();
                double value = 0d;

                try {
                    value = struct.getFloat64(VALUE);
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

            public Double merge(final Double value1, final Double value2) {
                throw new RuntimeException("Merge is not supported");
            }

            public Double map(final Double value) {
                return value;
            }
        };
    }

    private static void printException(String message) {
        RuntimeException runtimeException = new RuntimeException(message);
        runtimeException.printStackTrace();
    }
}