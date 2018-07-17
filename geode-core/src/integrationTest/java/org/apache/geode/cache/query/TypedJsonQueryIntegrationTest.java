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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.json.TypedJson;
import org.apache.geode.management.internal.cli.json.TypedJsonPdxIntegrationTest;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * Integration tests for {@link TypedJson} querying {@link Portfolio}.
 * <p>
 *
 * Extracted from {@link TypedJsonPdxIntegrationTest}.
 * <p>
 *
 * TODO: add real assertions
 */
@Category({OQLQueryTest.class})
public class TypedJsonQueryIntegrationTest {

  private static final String RESULT = "result";

  @Test
  public void testUserObject() throws Exception {
    Portfolio p = new Portfolio(2);

    TypedJson typedJson = new TypedJson(RESULT, p);

    checkResult(typedJson);
  }

  @Test
  public void testUserObjectArray() throws Exception {
    Portfolio[] portfolios = createPortfoliosAndPositions(2);

    TypedJson typedJson = new TypedJson(RESULT, portfolios);

    checkResult(typedJson);
  }

  @Test
  public void testMemUsage() throws Exception {
    Portfolio[] portfolios = createPortfoliosAndPositions(1000);
    System.out.println("Size Of port " + ObjectSizer.REFLECTION_SIZE.sizeof(portfolios));

    TypedJson typedJson = new TypedJson(RESULT, portfolios);
    System.out.println("Size Of json " + ObjectSizer.REFLECTION_SIZE.sizeof(typedJson));

    checkResult(typedJson);
  }

  @Test
  public void testQueryLike() throws Exception {
    Portfolio[] portfolios = createPortfoliosAndPositions(2);

    TypedJson typedJson = new TypedJson(RESULT, null);
    typedJson.add("member", "server1");
    // checkResult(typedJson); -- fails

    for (int i = 0; i < 2; i++) {
      typedJson.add(RESULT, portfolios[i]);
    }
    checkResult(typedJson);
  }

  private Portfolio[] createPortfoliosAndPositions(final int count) {
    Position.cnt = 0; // reset Portfolio counter
    Portfolio[] portfolios = new Portfolio[count];
    for (int i = 0; i < count; i++) {
      portfolios[i] = new Portfolio(i);
    }
    return portfolios;
  }

  private void checkResult(final TypedJson typedJson) throws GfJsonException {
    GfJsonObject gfJsonObject = new GfJsonObject(typedJson.toString());
    System.out.println(gfJsonObject);
    assertThat(gfJsonObject.get(RESULT)).isNotNull();
  }
}
