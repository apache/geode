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
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexUsageInNestedQueryWithParamsJUnitTest {
  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region portfolioRegion =
        CacheUtils.createRegion("portfolio", PortfolioForParameterizedQueries.class);
    Region intBoundRegion = CacheUtils.createRegion("intBound", IntermediateBound.class);
    QueryService queryService = CacheUtils.getQueryService();
    queryService.defineIndex("portfolioIdIndex", "p.ID", SEPARATOR + "portfolio p");
    queryService.defineIndex("temporaryOriginalBoundIdIdx", "ib.originalId",
        SEPARATOR + "intBound ib");
    queryService.createDefinedIndexes();
    PortfolioForParameterizedQueries p = new PortfolioForParameterizedQueries(1L);
    p.setDataList(Arrays
        .asList(new PortfolioData(Arrays.asList(new Bound(11L), new Bound(12L))),
            new PortfolioData(Arrays.asList(new Bound(21L), new Bound(22L)))));
    portfolioRegion.put(p.getID(), p);
    intBoundRegion.put(91L, new IntermediateBound(91L, 12L, new Bound(91L)));
    intBoundRegion.put(92L, new IntermediateBound(92L, 21L, new Bound(92L)));
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void whenParameterizedQueryExecutedThenItShouldReturnCorrectResultsAndNotThrowException()
      throws Exception {

    QueryService queryService = CacheUtils.getQueryService();
    Query query = queryService.newQuery(
        "<TRACE>select l.ID from " + SEPARATOR
            + "portfolio p, p.dataList dl, ($1).getConcatenatedBoundsAndIntermediateBounds(dl) l WHERE p.ID IN SET(1L,2L)");
    QueryParameterProvider querySupportService = new QueryParameterProvider(CacheUtils.getCache());
    Object[] params = new Object[] {querySupportService.createQueryParameter()};
    SelectResults result = (SelectResults) query.execute(params);
    assertEquals("The query returned a wrong result set = " + result.asList(), true,
        result.containsAll(new ArrayList<>(Arrays.asList(21L, 22L, 92L, 12L, 11L, 91L))));
  }


  public class PortfolioForParameterizedQueries implements Serializable {
    long ID;
    Collection<PortfolioData> dataList = new ArrayList<>();

    public long getID() {
      return ID;
    }

    public void setID(long ID) {
      this.ID = ID;
    }

    public Collection<PortfolioData> getDataList() {
      return dataList;
    }

    public void setDataList(
        Collection<PortfolioData> dataList) {
      this.dataList = dataList;
    }

    public PortfolioForParameterizedQueries(long ID) {
      this.ID = ID;
    }

    @Override
    public String toString() {
      return "PortfolioParameter{" + "id=" + ID + ", data=" + dataList + "}";
    }
  }

  public class PortfolioData {
    List<Bound> bounds = new ArrayList<>();

    public List<Bound> getBound() {
      return bounds;
    }

    public void setLimits(List<Bound> bounds) {
      this.bounds = bounds;
    }

    public PortfolioData(List<Bound> bounds) {
      this.bounds = bounds;
    }

    @Override
    public String toString() {
      return "PortfolioData{bounds=" +
          bounds +
          '}';
    }
  }

  public class Bound implements Serializable {
    long ID;

    public Bound(long ID) {
      this.ID = ID;
    }

    public long getID() {
      return ID;
    }

    public void setID(long ID) {
      this.ID = ID;
    }

    @Override
    public String toString() {
      return "Bound{ID=" + ID + "}";
    }
  }

  public class QueryParameterProvider {
    private final Cache cache;

    public QueryParameterProvider(Cache cache) {
      this.cache = cache;
    }

    public QueryParameter createQueryParameter() {
      return new QueryParameter(
          "<TRACE>select distinct ib from " + SEPARATOR + "intBound ib where ib.originalId in $1");
    }

    public class QueryParameter {
      private final ExecutorService executorService = Executors.newSingleThreadExecutor();
      private final Query query;

      private QueryParameter(final String queryString) {
        try {
          query = executorService
              .submit(() -> cache.getQueryService().newQuery(queryString)).get();
        } catch (InterruptedException e) {
          throw new RuntimeException("Thread was unexpectedly interrupted", e);
        } catch (ExecutionException e) {
          throw new RuntimeException("Could not create query context", e);
        }
      }

      @SuppressWarnings("unchecked")
      private Iterable<Bound> getIntermediateBounds(final Set<Long> originalBoundIds) {
        final Set<IntermediateBound> infos;
        final List<Bound> intermediateBounds = new ArrayList<>();

        try {
          infos = executorService.submit(
              () -> ((SelectResults<IntermediateBound>) query
                  .execute(new Object[] {originalBoundIds}))
                      .asSet())
              .get();
        } catch (InterruptedException e) {
          throw new RuntimeException("Thread was unexpectedly interrupted", e);
        } catch (ExecutionException e) {
          throw new RuntimeException("Could not query for temp limits", e);
        }

        if (infos != null) {
          for (final IntermediateBound info : infos) {
            if (info != null) {
              final Bound intLimit = info.getBound();
              if (intLimit != null) {
                intermediateBounds.add(intLimit);
              }
            }
          }
        }
        return intermediateBounds;
      }

      public Collection<Bound> getConcatenatedBoundsAndIntermediateBounds(
          final PortfolioData portfolioData) {
        final Map<Long, Bound> boundMap = getBounds(portfolioData);
        return Lists.newArrayList(
            Iterables.concat(boundMap.values(), getIntermediateBounds(boundMap.keySet())));
      }

      private Map<Long, Bound> getBounds(final PortfolioData portfolioData) {
        final Map<Long, Bound> boundsMap = new HashMap<>();
        if (portfolioData != null) {
          for (final Bound bound : portfolioData.bounds) {
            if (bound != null) {
              boundsMap.put(bound.ID, bound);
            }
          }
        }
        return boundsMap;
      }
    }
  }

  public class IntermediateBound implements Serializable {
    long ID;
    long originalId;
    Bound bound;

    public long getOriginalId() {
      return originalId;
    }

    public long getID() {
      return ID;
    }

    public void setID(long ID) {
      this.ID = ID;
    }

    public Bound getBound() {
      return bound;
    }

    public IntermediateBound setOriginalId(long originalId) {
      this.originalId = originalId;
      return this;
    }

    public IntermediateBound setBound(Bound bound) {
      this.bound = bound;
      return this;
    }

    public IntermediateBound(long ID, long originalId, Bound bound) {
      this.ID = ID;
      this.originalId = originalId;
      this.bound = bound;
    }

    @Override
    public String toString() {
      return "Bound{" +
          "id=" + ID +
          '}';
    }

  }
}
