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
package org.apache.geode.management.internal.cli.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;


/**
 * Extracted from {@link TypedJsonPdxIntegrationTest}.
 * <p>
 *
 * TODO: add actual assertions
 */
public class QueryResultFormatterTest {

  private static final String RESULT = "result";

  @Test
  public void canBeMocked() throws Exception {
    QueryResultFormatter mockQueryResultFormatter = mock(QueryResultFormatter.class);
    Object value = new Object();

    mockQueryResultFormatter.add("key", value);

    verify(mockQueryResultFormatter, times(1)).add("key", value);
  }

  @Test
  public void testArrayList() throws Exception {
    List<String> list = new ArrayList<>();
    list.add("ONE");
    list.add("TWO");
    list.add("THREE");

    QueryResultFormatter queryResultFormatter = new QueryResultFormatter(100).add(RESULT, list);

    checkResult(queryResultFormatter);
  }

  @Test
  public void testArray() throws Exception {
    int[] intArray = new int[3];
    for (int i = 0; i < 3; i++) {
      intArray[i] = i;
    }

    QueryResultFormatter queryResultFormatter = new QueryResultFormatter(100).add(RESULT, intArray);

    checkResult(queryResultFormatter);
  }

  @Test
  public void testBigList() throws Exception {
    List<String> list = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      list.add("BIG_COLL_" + i);
    }

    QueryResultFormatter queryResultFormatter = new QueryResultFormatter(100).add(RESULT, list);

    checkResult(queryResultFormatter);
  }

  @Test
  public void testEnumContainer() throws Exception {
    EnumContainer enumContainer = new EnumContainer(Currency.DIME);

    QueryResultFormatter queryResultFormatter =
        new QueryResultFormatter(100).add(RESULT, enumContainer);

    checkResult(queryResultFormatter);
  }

  @Test
  public void testEnum() throws Exception {
    QueryResultFormatter queryResultFormatter =
        new QueryResultFormatter(100).add(RESULT, Currency.DIME);

    checkResult(queryResultFormatter);
  }

  @Test
  public void testEnumList() throws Exception {
    List<Currency> list = new ArrayList();
    list.add(Currency.DIME);
    list.add(Currency.NICKLE);
    list.add(Currency.QUARTER);
    list.add(Currency.NICKLE);

    QueryResultFormatter queryResultFormatter = new QueryResultFormatter(100).add(RESULT, list);

    checkResult(queryResultFormatter);
  }

  @Test
  public void testMap() throws Exception {
    Map<String, String> map = new HashMap<>();
    map.put("1", "ONE");
    map.put("2", "TWO");
    map.put("3", "THREE");
    map.put("4", "FOUR");

    QueryResultFormatter queryResultFormatter = new QueryResultFormatter(100).add(RESULT, map);

    checkResult(queryResultFormatter);
  }

  @Test
  public void testBigDecimal() throws Exception {
    BigDecimal dc = new BigDecimal(20);

    QueryResultFormatter queryResultFormatter = new QueryResultFormatter(100).add(RESULT, dc);

    checkResult(queryResultFormatter);
  }

  @Test
  public void testObjects() throws Exception {
    Object object = new Object();

    QueryResultFormatter queryResultFormatter = new QueryResultFormatter(100);
    queryResultFormatter.add(RESULT, object);
    checkResult(queryResultFormatter);
  }

  private void checkResult(final QueryResultFormatter queryResultFormatter) throws GfJsonException {
    String jsonString = queryResultFormatter.toString();
    System.out.println("queryResultFormatter.toString=" + jsonString);
    GfJsonObject gfJsonObject = new GfJsonObject(jsonString);
    System.out.println("gfJsonObject=" + gfJsonObject);
    assertThat(gfJsonObject.get(RESULT)).isNotNull();
  }

  private enum Currency {
    PENNY, NICKLE, DIME, QUARTER
  }

  private static class EnumContainer {

    private final Currency currency;

    EnumContainer(final Currency currency) {
      this.currency = currency;
    }
  }
}
