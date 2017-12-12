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
package org.apache.geode.connectors.jdbc.internal;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AbstractJdbcCallbackTest {

  private AbstractJdbcCallback jdbcCallback;
  private SqlHandler sqlHandler;

  @Before
  public void setUp() throws Exception {
    sqlHandler = mock(SqlHandler.class);
    jdbcCallback = new AbstractJdbcCallback(sqlHandler) {};
  }

  @Test
  public void closesSqlHandler() throws Exception {
    jdbcCallback.close();
    verify(sqlHandler, times(1)).close();
  }

  @Test
  public void returnsCorrectSqlHander() throws Exception {
    assertThat(jdbcCallback.getSqlHandler()).isSameAs(sqlHandler);
  }

  @Test
  public void checkInitializedDoesNothingIfInitialized() {
    jdbcCallback.checkInitialized(mock(InternalCache.class));
    assertThat(jdbcCallback.getSqlHandler()).isSameAs(sqlHandler);
  }

  @Test
  public void initializedSqlHandlerIfNoneExists() {
    jdbcCallback = new AbstractJdbcCallback() {};
    InternalCache cache = mock(InternalCache.class);
    InternalJdbcConnectorService service = mock(InternalJdbcConnectorService.class);
    when(cache.getService(any())).thenReturn(service);
    assertThat(jdbcCallback.getSqlHandler()).isNull();

    jdbcCallback.checkInitialized(cache);

    assertThat(jdbcCallback.getSqlHandler()).isNotNull();
  }
}
