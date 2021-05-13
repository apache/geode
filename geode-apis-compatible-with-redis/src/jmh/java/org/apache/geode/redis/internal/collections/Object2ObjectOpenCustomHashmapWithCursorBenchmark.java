package org.apache.geode.redis.internal.collections;

import java.util.Iterator;
import java.util.Random;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

public class Object2ObjectOpenCustomHashmapWithCursorBenchmark {

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    @Param({"1000", "10000", "100000"})
    private int numEntries;
    @Param({"32"})
    private int keySize;
    private Object2ObjectOpenCustomHashMapWithCursor<byte[], byte[]> map;
    private int cursor;
    private Iterator<byte[]> iterator;

    @Setup
    public void createMap() {
      Random random = new Random(0);
      map = new Object2ObjectOpenCustomHashMapWithCursor<>(numEntries, ByteArrays.HASH_STRATEGY);
      for (int i = 0; i < numEntries; i++) {
        byte[] key = new byte[keySize];
        random.nextBytes(key);
        map.put(key, key);
      }

      iterator = map.keySet().iterator();
    }
  }

  @Benchmark
  public void iterateWithCursor(BenchmarkState state, Blackhole blackhole) {
    state.cursor =
        state.map.scan(state.cursor, 1, (hole, key, value) -> hole.consume(key), blackhole);
  }

  @Benchmark
  public void iterateWithIterator(BenchmarkState state, Blackhole blackhole) {
    if (!state.iterator.hasNext()) {
      state.iterator = state.map.keySet().iterator();
    }

    blackhole.consume(state.iterator.next());
  }


}
