package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CheckpointSumTest {

  private static final Schema INPUT_STRUCT = SchemaBuilder.struct().optional()
    .field(CheckpointSum.TYPE, Schema.OPTIONAL_STRING_SCHEMA)
    .field(CheckpointSum.VALUE, Schema.OPTIONAL_FLOAT32_SCHEMA)
    .build();

  @Test
  public void test() {
    Udaf<Struct, Struct, Float> udaf = CheckpointSum.checkpointSum();
    Struct aggregate = udaf.initialize();

    Struct[] values = new Struct[] {
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0f),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0f),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0f)
    };

    for (Struct thisValue: values) {
      log("value = [" + aggregate.getFloat32(CheckpointSum.VALUE) + "]");
      aggregate = udaf.aggregate(thisValue, aggregate);
    }

    log("value = [" + aggregate.getFloat32(CheckpointSum.VALUE) + "]");
    assertEquals(3.0f, aggregate.getFloat32(CheckpointSum.VALUE), 0);

    aggregate = udaf.aggregate(aggregate, new Struct(INPUT_STRUCT).put(
        CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0f));

    log("value = [" + aggregate.getFloat32(CheckpointSum.VALUE) + "]");
    assertEquals(4.0f, aggregate.getFloat32(CheckpointSum.VALUE), 0);

    aggregate = udaf.aggregate(aggregate, new Struct(INPUT_STRUCT).put(
        CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0f));

    log("value = [" + aggregate.getFloat32(CheckpointSum.VALUE) + "]");
    assertEquals(5.0f, aggregate.getFloat32(CheckpointSum.VALUE), 0);

    log("reset to absolute...");

    aggregate = udaf.aggregate(aggregate, new Struct(INPUT_STRUCT).put(
        CheckpointSum.TYPE, CheckpointSum.TYPE_ABSOLUTE).put(CheckpointSum.VALUE, 0.0f));

    log("value = [" + aggregate.getFloat32(CheckpointSum.VALUE) + "]");

    aggregate = udaf.aggregate(aggregate, new Struct(INPUT_STRUCT).put(
        CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0f));

    log("value = [" + aggregate.getFloat32(CheckpointSum.VALUE) + "]");
    assertEquals(1.0f, aggregate.getFloat32(CheckpointSum.VALUE), 0);

    aggregate = udaf.aggregate(aggregate, new Struct(INPUT_STRUCT).put(
        CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0f));

    log("value = [" + aggregate.getFloat32(CheckpointSum.VALUE) + "]");
    assertEquals(2.0f, aggregate.getFloat32(CheckpointSum.VALUE), 0);
  }

  private static void log(String value) {
    System.out.println(value);
  }
}
