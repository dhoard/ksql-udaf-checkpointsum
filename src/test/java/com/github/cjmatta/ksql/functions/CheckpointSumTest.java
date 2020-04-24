package com.github.cjmatta.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CheckpointSumTest {

  @Test
  public void test() {
    Schema INPUT_STRUCT = SchemaBuilder.struct().optional()
        .field(CheckpointSum.TYPE, Schema.STRING_SCHEMA)
        .field(CheckpointSum.VALUE, Schema.FLOAT64_SCHEMA)
        .build();

    Udaf<Struct, Struct, Double> udaf = CheckpointSum.checkpointSum();
    Struct aggregate = udaf.initialize();

    Struct[] values = new Struct[] {
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d),
      new Struct(INPUT_STRUCT).put(CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d)
    };

    for (Struct thisValue: values) {
      log("value = [" + aggregate.getFloat64(CheckpointSum.VALUE) + "]");
      aggregate = udaf.aggregate(thisValue, aggregate);
    }

    log("value = [" + aggregate.getFloat64(CheckpointSum.VALUE) + "]");
    assertEquals(3.0f, aggregate.getFloat64(CheckpointSum.VALUE), 0);

    aggregate = udaf.aggregate(new Struct(INPUT_STRUCT).put(
        CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d), aggregate);

    log("value = [" + aggregate.getFloat64(CheckpointSum.VALUE) + "]");
    assertEquals(4.0f, aggregate.getFloat64(CheckpointSum.VALUE), 0);

    aggregate = udaf.aggregate(new Struct(INPUT_STRUCT).put(
        CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d), aggregate);

    log("value = [" + aggregate.getFloat64(CheckpointSum.VALUE) + "]");
    assertEquals(5.0f, aggregate.getFloat64(CheckpointSum.VALUE), 0);

    log("reset to absolute...");

    aggregate = udaf.aggregate(new Struct(INPUT_STRUCT).put(
        CheckpointSum.TYPE, CheckpointSum.TYPE_ABSOLUTE).put(CheckpointSum.VALUE, 0.0d), aggregate);

    log("value = [" + aggregate.getFloat64(CheckpointSum.VALUE) + "]");

    aggregate = udaf.aggregate(new Struct(INPUT_STRUCT).put(
        CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d), aggregate);

    log("value = [" + aggregate.getFloat64(CheckpointSum.VALUE) + "]");
    assertEquals(1.0f, aggregate.getFloat64(CheckpointSum.VALUE), 0);

    aggregate = udaf.aggregate(new Struct(INPUT_STRUCT).put(
        CheckpointSum.TYPE, CheckpointSum.TYPE_DELTA).put(CheckpointSum.VALUE, 1.0d), aggregate);

    log("value = [" + aggregate.getFloat64(CheckpointSum.VALUE) + "]");
    assertEquals(2.0f, aggregate.getFloat64(CheckpointSum.VALUE), 0);

    log("reset to absolute...");

    aggregate = udaf.aggregate(new Struct(INPUT_STRUCT).put(
        CheckpointSum.TYPE, CheckpointSum.TYPE_ABSOLUTE).put(CheckpointSum.VALUE, 7.0d), aggregate);

    log("value = [" + aggregate.getFloat64(CheckpointSum.VALUE) + "]");
    assertEquals(7.0f, aggregate.getFloat64(CheckpointSum.VALUE), 0);

    log("reset to absolute...");

    aggregate = udaf.aggregate(new Struct(INPUT_STRUCT).put(
        CheckpointSum.TYPE, CheckpointSum.TYPE_ABSOLUTE).put(CheckpointSum.VALUE, 32.0d), aggregate);

    log("value = [" + aggregate.getFloat64(CheckpointSum.VALUE) + "]");
    assertEquals(32.0d, aggregate.getFloat64(CheckpointSum.VALUE), 0);
  }

  @Test
  public void test2() {
    Schema INPUT_STRUCT = SchemaBuilder.struct().optional()
        .field(CheckpointSum.TYPE, Schema.STRING_SCHEMA)
        .field(CheckpointSum.VALUE, Schema.FLOAT64_SCHEMA)
        .build();

    Udaf<Struct, Struct, Double> udaf = CheckpointSum.checkpointSum();
    Struct aggregate = udaf.initialize();

    aggregate = udaf.aggregate(new Struct(INPUT_STRUCT).put(
        CheckpointSum.TYPE, CheckpointSum.TYPE_ABSOLUTE).put(CheckpointSum.VALUE, 10.0d), aggregate);

    log("value = [" + aggregate.getFloat64(CheckpointSum.VALUE) + "]");
    assertEquals(10.0d, aggregate.getFloat64(CheckpointSum.VALUE), 0);
  }

  private static void log(String value) {
    System.out.println(value);
  }
}
