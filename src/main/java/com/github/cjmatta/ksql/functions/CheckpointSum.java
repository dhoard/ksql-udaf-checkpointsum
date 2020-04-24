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
      .field(TYPE, Schema.OPTIONAL_STRING_SCHEMA)
      .field(VALUE, Schema.OPTIONAL_FLOAT32_SCHEMA)
      .build();

  @UdafFactory(description = "Compute the sum or a series of records representing either the absolute value or delta",
      aggregateSchema = "STRUCT<TYPE varchar, VALUE float>")
  public static Udaf<Struct, Struct, Float> checkpointSum() {

    return new Udaf<Struct, Struct, Float>() {

      @Override
      public Struct initialize() {
        return new Struct(AGGREGATE_STRUCT).put(TYPE, TYPE_ABSOLUTE).put(VALUE, 0.0f);
      }

      @Override
      public Struct aggregate(final Struct input, final Struct aggregate) {
        final Object obj = input.get(TYPE);

        if (obj == null) {
          return null;
        }

        final String typeVal = obj.toString();

        if (!(typeVal.equals(TYPE_ABSOLUTE) || typeVal.equals(TYPE_DELTA))) {
          return null;
        }

        if (typeVal.equals(TYPE_ABSOLUTE)) {
          return aggregate.put(TYPE, TYPE_ABSOLUTE).put(VALUE, input.getFloat32(VALUE));
        } else {
          return aggregate
              .put(TYPE, TYPE_DELTA)
              .put(VALUE, Float.valueOf(aggregate.getFloat32(VALUE).floatValue() + input.getFloat32(VALUE).floatValue()));
        }
      }

      @Override
      public Struct merge(Struct agg1, Struct agg2) {
        final String agg1Type = agg1.get(TYPE).toString();
        final String agg2Type = agg2.get(TYPE).toString();

        if (agg2Type.equals(TYPE_ABSOLUTE)) {
          return agg2;
        } else {
          return agg2.put(TYPE, TYPE_DELTA)
              .put(VALUE, agg1.getFloat32(VALUE) + agg2.getFloat32(VALUE));
        }
      }

      @Override
      public Float map(Struct agg) {
        return agg.getFloat32(VALUE).floatValue();
      }
    };
  }
}
