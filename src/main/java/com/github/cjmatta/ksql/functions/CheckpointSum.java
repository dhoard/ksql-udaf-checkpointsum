package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "checkpoint_sum", description = "Computes SUM of a stream of records where some are the absolute value, and some are the delta")
public class CheckpointSum {

  public static final String TYPE = "TYPE";
  public static final String VALUE = "VALUE";
  public static final String TYPE_ABSOLUTE = "absolute";
  public static final String TYPE_DELTA = "delta";

  private static final Schema AGGREGATE_STRUCT = SchemaBuilder.struct().optional()
      .field(TYPE, Schema.STRING_SCHEMA)
      .field(VALUE, Schema.FLOAT64_SCHEMA)
      .build();

  @UdafFactory(description = "Compute the sum or a series of records representing either the absolute value or delta",
      paramSchema = "STRUCT<TYPE varchar, VALUE double>",
      aggregateSchema = "double")
  public static Udaf<Struct, Double, Double> checkpointSum() {

    return new Udaf<Struct, Double, Double>() {

      @Override
      public Double initialize() {
        return 0.0d;
      }

      @Override
      public Double aggregate(final Struct input, final Double aggregate) {
        Object obj = input.get(TYPE);
        if (obj == null) {
          return null;
        }

        String typeVal = obj.toString();

        if (!(typeVal.equals(TYPE_ABSOLUTE) || typeVal.equals(TYPE_DELTA))) {
          return null;
        }

        Double value = input.getFloat64(VALUE).doubleValue();

        if (typeVal.equals(TYPE_ABSOLUTE)) {
          return value;
        } else {
          return aggregate + value;
        }
      }

      @Override
      public Double merge(Double agg1, Double agg2) {
        /*
        String agg2Type = agg2.get(TYPE).toString();

        if (agg2Type.equals(TYPE_ABSOLUTE)) {
          return agg2;
        } else {
          return agg2.put(TYPE, TYPE_DELTA)
              .put(VALUE, agg1.getFloat64(VALUE) + agg2.getFloat64(VALUE));
        }
        */
        return agg2;
      }

      @Override
      public Double map(Double aggregate) {
        return aggregate;
      }
    };
  }
}
