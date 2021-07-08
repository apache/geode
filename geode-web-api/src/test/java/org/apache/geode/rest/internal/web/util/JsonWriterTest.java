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
package org.apache.geode.rest.internal.web.util;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Date;

import com.fasterxml.jackson.core.JsonGenerator;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.RestAPITest;

@Category({RestAPITest.class})
public class JsonWriterTest {
  @Test
  public void testWwriteArrayAsJson() throws IOException {
    Date value = new Date();
    assertThatThrownBy(
        () -> JsonWriter.writeArrayAsJson(mock(JsonGenerator.class), value, "myDate"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Expected an array for pdx field myDate, but got an object of type "
                + value.getClass());

    Date values[] = new Date[2];
    assertThatThrownBy(
        () -> JsonWriter.writeArrayAsJson(mock(JsonGenerator.class), values, "myDates"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("The pdx field myDates is an array whose component type "
                + values.getClass().getComponentType()
                + " can not be converted to JSON.");
  }
}
