/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.lucene;

import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

/**
 * This class contains tests of lucene queries that can fit
 */
@Category(IntegrationTest.class)
public class LuceneQueriesIntegrationTest extends LuceneIntegrationTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  private static final String INDEX_NAME = "index";
  protected static final String REGION_NAME = "index";

  @Test()
  public void throwFunctionExceptionWhenGivenBadQuery() {
    LuceneService luceneService = LuceneServiceProvider.get(cache);
    luceneService.createIndex(INDEX_NAME, REGION_NAME, "text");
    Region region = cache.createRegionFactory(RegionShortcut.PARTITION)
      .create(REGION_NAME);

    //Create a query that throws an exception
    final LuceneQuery<Object, Object> query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
      index -> {
        throw new QueryException("Bad query");
      });


    thrown.expect(FunctionException.class);
    thrown.expectCause(isA(QueryException.class));
    try {
      query.search();
    } catch(FunctionException e) {
      assertEquals(QueryException.class, e.getCause().getClass());
      throw e;
    }

  }


}
