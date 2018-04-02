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
package org.apache.geode.experimental.driver;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.UUID;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class ValueSerializerIntegrationTest extends IntegrationTestBase {

  private JavaSerializer serializer;

  @Override
  protected Driver createDriver(int locatorPort) throws Exception {
    serializer = spy(new JavaSerializer());
    return new DriverFactory().addLocator("localhost", locatorPort).setValueSerializer(serializer)
        .create();
  }

  @Test
  public void testSerializerUsedForPut() throws IOException, ClassNotFoundException {
    Region<String, UUID> region = driver.getRegion("region");

    UUID value = UUID.randomUUID();
    region.put("key", value);

    assertEquals(value, region.get("key"));

    verify(serializer).deserialize(any());
  }


}
