package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class CheckpointSumTest {
  private static final String TYPE = "TYPE";
  private static final String VALUE = "VALUE";
  private static final String TYPE_ABSOLUTE = "absolute";
  private static final String TYPE_DELTA = "delta";

  private static final Schema INPUT_STRUCT = SchemaBuilder.struct().optional()
    .field(TYPE, Schema.OPTIONAL_STRING_SCHEMA)
    .field(VALUE, Schema.OPTIONAL_FLOAT32_SCHEMA)
    .build();

  @Test
  public void shouldSumDeltas() {
    final Udaf<Struct, Struct, Double> udaf = CheckpointSum.checkpointSum();
    Struct agg = udaf.initialize();

    final Struct[] values = new Struct[] {
      new Struct(INPUT_STRUCT).put(TYPE, TYPE_DELTA).put(VALUE, 1.0f),
      new Struct(INPUT_STRUCT).put(TYPE, TYPE_DELTA).put(VALUE, 1.0f),
      new Struct(INPUT_STRUCT).put(TYPE, TYPE_DELTA).put(VALUE, 1.0f)
    };

    for (final Struct thisValue: values) {
      agg = udaf.aggregate(thisValue, agg);
    }

    assertEquals(3.0, (double)agg.getFloat32(VALUE), 0);

  }
}
