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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;

@Fork(10)
public class RegionOperationBenchmark {

  @State(Scope.Benchmark)
  public static class CacheState {
    private Region region;

    @Setup
    public void setup() {
      Cache cache = new CacheFactory().set("mcast-port", "0").set("locators", "").create();

      region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("region");
    }
  }

  @Benchmark
  @Warmup(iterations = 20)
  @Measurement(iterations = 20)
  public Object put(CacheState state) throws NameResolutionException, TypeMismatchException,
      QueryInvocationTargetException, FunctionDomainException {
    return state.region.put("key", "value");
  }
}
