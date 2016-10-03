/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache.query.internal.index;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class RangeIndexMaintenanceIntegrationTest extends AbstractIndexMaintenanceIntegrationTest {

  @Override
  protected AbstractIndex createIndex(final QueryService qs, String name, String indexExpression, String regionPath)
    throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    boolean oldTestValue = IndexManager.TEST_RANGEINDEX_ONLY;
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    RangeIndex index = (RangeIndex)qs.createIndex(name, indexExpression, regionPath);
    IndexManager.TEST_RANGEINDEX_ONLY = oldTestValue;
    return index;
  }
}
