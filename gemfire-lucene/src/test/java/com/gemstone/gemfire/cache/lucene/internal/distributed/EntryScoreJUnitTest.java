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

package com.gemstone.gemfire.cache.lucene.internal.distributed;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class EntryScoreJUnitTest {
  @Test
  public void testSerialization() {
    LuceneServiceImpl.registerDataSerializables();
    EntryScore entry = new EntryScore("entry", .1f);
    EntryScore copy = CopyHelper.deepCopy(entry);
    Assert.assertEquals("entry", copy.getKey());
    Assert.assertEquals(.1f, copy.getScore(), 0f);
  }
}
