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
package org.apache.geode.connectors.jdbc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.connectors.jdbc.internal.SqlHandler;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.test.fake.Fakes;

public class JdbcLoaderTest {

  private SqlHandler sqlHandler;
  private LoaderHelper<Object, Object> loaderHelper;

  private JdbcLoader<Object, Object> loader;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    InternalCache cache = Fakes.cache();
    sqlHandler = mock(SqlHandler.class);
    loaderHelper = mock(LoaderHelper.class);

    when(loaderHelper.getRegion()).thenReturn(mock(InternalRegion.class));

    loader = new JdbcLoader<>(sqlHandler, cache);
  }

  @Test
  public void loadReadsFromSqlHandler() throws Exception {
    loader.load(loaderHelper);

    verify(sqlHandler, times(1)).read(any(), any());
  }
}
