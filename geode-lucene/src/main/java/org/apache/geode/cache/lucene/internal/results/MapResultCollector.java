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
package org.apache.geode.cache.lucene.internal.results;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MapResultCollector implements ResultCollector<List<PageEntry>, Map<Object, Object>> {
  private final Map<Object, Object> results = new HashMap<>();

  @Override
  public Map<Object, Object> getResult() throws FunctionException {
    return results;
  }

  @Override
  public Map<Object, Object> getResult(final long timeout, final TimeUnit unit)
      throws FunctionException, InterruptedException {
    return results;
  }

  @Override
  public void addResult(final DistributedMember memberID,
      final List<PageEntry> resultOfSingleExecution) {
    for (PageEntry entry : resultOfSingleExecution) {
      results.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void endResults() {

  }

  @Override
  public void clearResults() {
    results.clear();

  }
}
