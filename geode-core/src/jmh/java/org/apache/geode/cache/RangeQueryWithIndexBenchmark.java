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
package org.apache.geode.cache;

import static org.junit.Assert.assertEquals;

import java.util.stream.IntStream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.index.AbstractIndex;

@Fork(3)
public class RangeQueryWithIndexBenchmark {

  @State(Scope.Benchmark)
  public static class CacheState {
    private Region region;
    private Query query;

    public CacheState() {}

    @Setup
    public void setup() {
      Cache cache = new CacheFactory().set("mcast-port", "0").set("locators", "").create();

      region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("region");
      try {
        AbstractIndex index =
            (AbstractIndex) cache.getQueryService().createIndex("Status", "id", "/region");

        IntStream.range(0, 10000).forEach(i -> region.put(i, new Value(i)));
        query = cache.getQueryService().newQuery("select * from /region where id > 0");

        // Do the query once to make sure it's actually returning results
        // And using the index
        SelectResults results = query();
        assertEquals(9999, results.size());
        assertEquals(1, index.getStatistics().getTotalUses());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }

    public SelectResults query() throws NameResolutionException, TypeMismatchException,
        QueryInvocationTargetException, FunctionDomainException {
      return (SelectResults) query.execute();
    }
  }

  @Benchmark
  @Warmup(iterations = 20)
  @Measurement(iterations = 20)
  public Object query(CacheState state) throws NameResolutionException, TypeMismatchException,
      QueryInvocationTargetException, FunctionDomainException {
    return state.query();
  }



  public static class Value {
    protected final int id;

    public Value(int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }
  }
}
