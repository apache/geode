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
package org.apache.geode.pdx.internal.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Date;

import com.fasterxml.jackson.core.JsonGenerator;
import org.junit.Test;

import org.apache.geode.pdx.PdxInstance;

public class PdxToJSONTest {

  @Test
  public void testWriteValueAsJsonException() throws IOException {
    PdxToJSON p2j = new PdxToJSON(mock(PdxInstance.class));
    Date value = new Date();
    try {
      p2j.writeValue(mock(JsonGenerator.class), value, "myDate");
      fail("should not succeed");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).contains("The pdx field myDate has a value " + value
          + " whose type " + value.getClass() + " can not be converted to JSON.");
    }
  }

  @Test
  public void testGetJSONStringFromArray() throws IOException {
    PdxToJSON p2j = new PdxToJSON(mock(PdxInstance.class));
    Date value = new Date();
    try {
      p2j.getJSONStringFromArray(mock(JsonGenerator.class), value, "myDate");
      fail("should not succeed");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage())
          .contains("Expected an array for pdx field myDate, but got an object of type "
              + value.getClass());
    }

    Date values[] = new Date[2];
    try {
      p2j.getJSONStringFromArray(mock(JsonGenerator.class), values, "myDates");
      fail("should not succeed");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).contains("The pdx field myDates is an array whose component type "
          + values.getClass().getComponentType()
          + " can not be converted to JSON.");
    }
  }
}
