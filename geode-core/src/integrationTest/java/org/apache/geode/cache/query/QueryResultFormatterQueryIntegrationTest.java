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
package org.apache.geode.cache.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.management.internal.json.QueryResultFormatter;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * Integration tests for {@link QueryResultFormatter} querying {@link Portfolio}.
 * Extracted from {@code QueryResultFormatterPdxIntegrationTest}
 *
 * TODO: add real assertions
 */
@Category({OQLQueryTest.class})
public class QueryResultFormatterQueryIntegrationTest {

  private static final String RESULT = "result";

  @Test
  public void testUserObject() throws Exception {
    Portfolio p = new Portfolio(2);

    QueryResultFormatter queryResultFormatter = new QueryResultFormatter(100).add(RESULT, p);

    checkResult(queryResultFormatter);
  }

  @Test
  public void testUserObjectArray() throws Exception {
    Portfolio[] portfolios = createPortfoliosAndPositions(2);

    QueryResultFormatter queryResultFormatter =
        new QueryResultFormatter(100).add(RESULT, portfolios);

    checkResult(queryResultFormatter);
  }

  @Test
  public void testMemUsage() throws Exception {
    Portfolio[] portfolios = createPortfoliosAndPositions(1000);
    System.out.println("Size Of port " + ObjectSizer.REFLECTION_SIZE.sizeof(portfolios));

    QueryResultFormatter queryResultFormatter =
        new QueryResultFormatter(100).add(RESULT, portfolios);
    System.out.println("Size Of json " + ObjectSizer.REFLECTION_SIZE.sizeof(queryResultFormatter));

    checkResult(queryResultFormatter);
  }

  @Test
  public void testQueryLike() throws Exception {
    Portfolio[] portfolios = createPortfoliosAndPositions(2);

    QueryResultFormatter queryResultFormatter = new QueryResultFormatter(100).add(RESULT, null);
    queryResultFormatter.add("member", "server1");
    // checkResult(queryResultFormatter); -- fails

    for (int i = 0; i < 2; i++) {
      queryResultFormatter.add(RESULT, portfolios[i]);
    }
    checkResult(queryResultFormatter);
  }

  private Portfolio[] createPortfoliosAndPositions(final int count) {
    Position.cnt = 0; // reset Portfolio counter
    Portfolio[] portfolios = new Portfolio[count];
    for (int i = 0; i < count; i++) {
      portfolios[i] = new Portfolio(i);
    }
    return portfolios;
  }

  private void checkResult(final QueryResultFormatter queryResultFormatter) throws IOException {
    JsonNode jsonObject = new ObjectMapper().readTree(queryResultFormatter.toString());
    System.out.println(jsonObject.toString());
    assertThat(jsonObject.get(RESULT)).isNotNull();
  }
}
