/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.internal;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Category(UnitTest.class)
public class LuceneServiceImplJUnitTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  Region region;
  GemFireCacheImpl cache;
  LuceneServiceImpl service = new LuceneServiceImpl();

  @Before
  public void createMocks() throws NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException {
    region = mock(Region.class);
    cache = mock(GemFireCacheImpl.class);
    Field f = LuceneServiceImpl.class.getDeclaredField("cache");
    f.setAccessible(true);
    f.set(service, cache);
  }

  @Test
  public void shouldThrowIllegalArgumentExceptionIfFieldsAreMissing() {
    thrown.expect(IllegalArgumentException.class);
    service.createIndexFactory().create("index", "region");
  }

  @Test
  public void shouldThrowIllegalArgumentExceptionIfFieldsMapIsMissing() {
    thrown.expect(IllegalArgumentException.class);
    service.createIndex("index", "region", Collections.emptyMap());
  }

  @Test
  public void shouldReturnFalseIfRegionNotFoundInWaitUntilFlush() throws InterruptedException {
    boolean result =
        service.waitUntilFlushed("dummyIndex", "dummyRegion", 60000, TimeUnit.MILLISECONDS);
    assertFalse(result);
  }

}
