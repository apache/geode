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

package org.apache.geode.benchmark.jmh.profilers;

import static org.apache.geode.benchmark.jmh.profilers.ObjectSizeAgent.sizeOfDeep;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

/**
 * Profiles the size of the registered objects. A deep size calculation is performed after an
 * iteration to determine the retained size of the objects.
 */
public class ObjectSizeProfiler implements InternalProfiler {

  private static final HashMap<String, Object> objects = new HashMap<>();
  private static boolean enabled = false;

  /**
   * Registers the named object for size profiling.
   *
   * @param name of object to size.
   * @param object to size.
   */
  public static void objectSize(final String name, final Object object) {
    if (!enabled) {
      return;
    }

    objects.put(name, object);
  }

  @Override
  public void beforeIteration(final BenchmarkParams benchmarkParams,
      final IterationParams iterationParams) {
    enabled = true;
  }

  @Override
  public Collection<? extends Result<?>> afterIteration(final BenchmarkParams benchmarkParams,
      final IterationParams iterationParams,
      final IterationResult result) {
    final List<Result<?>> results = new ArrayList<>();
    for (Map.Entry<String, Object> entry : objects.entrySet()) {
      result.addResult(new ScalarResult("objectSize." + entry.getKey(),
          sizeOfDeep(entry.getValue()), "bytes", AggregationPolicy.AVG));
    }
    return results;
  }

  @Override
  public String getDescription() {
    return "Adds size of objects after iteration.";
  }

}
