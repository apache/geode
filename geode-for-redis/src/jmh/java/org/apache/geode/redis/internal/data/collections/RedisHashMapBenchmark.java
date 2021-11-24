/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.redis.internal.data.collections;

import java.util.Iterator;
import java.util.Random;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.geode.redis.internal.data.RedisHash;

public class RedisHashMapBenchmark {

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    @Param({"1000", "10000", "100000"})
    private int numEntries;
    @Param({"32"})
    private int keySize;
    private RedisHash.Hash map;
    private int cursor;
    private Iterator<byte[]> iterator;

    @Setup
    public void createMap() {
      Random random = new Random(0);
      map = new RedisHash.Hash(numEntries);
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
