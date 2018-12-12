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
package org.apache.geode.test.matchers;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONCompare;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.JSONCompareResult;

/**
 * Matcher which tests equivalence between json documents. The comparison does not consider array
 * ordering. An example might be:
 *
 * <p/>
 *
 * <pre>
 * mockMvc.perform(get("/v1/customers?limit=10"))
 *     .andExpect(status().isOk())
 *     .andExpect(jsonPath("$.customers", jsonEquals(jsonResources.get(CUSTOMER_LIST_JSON))));
 * </pre>
 */
public class JsonEquivalence extends BaseMatcher {
  private final String expectedJson;

  private String failureMessage;

  private JsonEquivalence(String expectedJson) {
    this.expectedJson = expectedJson;
  }

  @Override
  public boolean matches(Object item) {
    if (!(item instanceof String)) {
      ObjectMapper om = new ObjectMapper();
      try {
        item = om.writeValueAsString(item);
      } catch (JsonProcessingException e) {
        failureMessage = e.getMessage();
        return false;
      }
    }

    JSONCompareResult result;
    try {
      result = JSONCompare.compareJSON(expectedJson, item.toString(), JSONCompareMode.LENIENT);
    } catch (JSONException jex) {
      failureMessage = jex.getMessage();
      return false;
    }

    if (result != null && result.failed()) {
      failureMessage = result.getMessage();
    }

    return result != null && result.passed();
  }

  @Override
  public void describeTo(Description description) {
    description.appendText(failureMessage);
  }

  public static Matcher jsonEquals(String operand) {
    return new JsonEquivalence(operand);
  }
}
