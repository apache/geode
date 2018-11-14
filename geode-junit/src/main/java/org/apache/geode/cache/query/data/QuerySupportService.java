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
package org.apache.geode.cache.query.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.SelectResults;

public class QuerySupportService {

  private Cache gemfireCache;

  public QuerySupportService(Cache gemfireCache) {
    this.gemfireCache = gemfireCache;
  }

  public QueryContext createQueryContext() {
    return new QueryContext(
        "<TRACE>select distinct tl from /TempLimit tl where tl.originalId in $1");
  }

  public class QueryContext {
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final Query query;

    private QueryContext(final String queryString) {
      try {
        this.query = executorService
            .submit(() -> gemfireCache.getQueryService().newQuery(queryString)).get();
      } catch (InterruptedException e) {
        throw new RuntimeException("Thread was unexpectedly interrupted", e);
      } catch (ExecutionException e) {
        throw new RuntimeException("Could not create query context", e);
      }
    }

    @SuppressWarnings("unchecked")
    private Iterable<Limit> getTempLimits(final Set<Long> originalLimitIds) {
      final Set<TempLimit> infos;
      final List<Limit> tempLimits = new ArrayList<>();
      System.out.println("###################### originalLimitIds: " + originalLimitIds);

      try {
        infos = executorService.submit(
            () -> ((SelectResults<TempLimit>) query.execute(new Object[] {originalLimitIds}))
                .asSet())
            .get();
      } catch (InterruptedException e) {
        throw new RuntimeException("Thread was unexpectedly interrupted", e);
      } catch (ExecutionException e) {
        throw new RuntimeException("Could not query for temp limits", e);
      }

      if (infos != null) {
        for (final TempLimit info : infos) {
          if (info != null) {
            final Limit tempLimit = info.getLimit();
            if (tempLimit != null) {
              tempLimits.add(tempLimit);
            }
          }
        }
      }
      return tempLimits;
    }

    public Collection<Limit> getLimitsAndTempLimits(final PortfolioMetric portfolioMetric) {
      final Map<Long, Limit> limits = getLimits(portfolioMetric);
      return Lists.newArrayList(Iterables.concat(limits.values(), getTempLimits(limits.keySet())));
    }

    private Map<Long, Limit> getLimits(final PortfolioMetric portfolioMetric) {
      final Map<Long, Limit> limits = new HashMap<Long, Limit>();
      if (portfolioMetric != null) {
        for (final Limit limit : portfolioMetric.limits) {
          if (limit != null) {
            limits.put(limit.id, limit);
          }
        }
      }
      return limits;
    }
  }
}
