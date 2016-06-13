/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.gemstone.gemfire.cache.lucene.internal;

import static org.junit.Assert.*;

import java.util.Collections;

import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category(UnitTest.class)
public class LuceneServiceImplJUnitTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void shouldThrowIllegalArgumentExceptionIfFieldsAreMissing() {
    LuceneServiceImpl service = new LuceneServiceImpl();
    thrown.expect(IllegalArgumentException.class);
    service.createIndex("index", "region");
  }

  @Test
  public void shouldThrowIllegalArgumentExceptionIfFieldsMapIsMissing() {
    LuceneServiceImpl service = new LuceneServiceImpl();
    thrown.expect(IllegalArgumentException.class);
    service.createIndex("index", "region", Collections.emptyMap());
  }


}