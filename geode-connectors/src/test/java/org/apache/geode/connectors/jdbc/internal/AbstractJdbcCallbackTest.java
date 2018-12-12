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
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.fake.Fakes;

public class AbstractJdbcCallbackTest {

  private AbstractJdbcCallback jdbcCallback;
  private SqlHandler sqlHandler;
  private InternalCache cache;

  @Before
  public void setUp() {
    cache = Fakes.cache();
    sqlHandler = mock(SqlHandler.class);
    jdbcCallback = new AbstractJdbcCallback(sqlHandler, cache) {};
  }

  @Test
  public void returnsCorrectSqlHander() {
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
    JdbcConnectorService service = mock(JdbcConnectorService.class);
    when(cache.getService(any())).thenReturn(service);
    assertThat(jdbcCallback.getSqlHandler()).isNull();

    jdbcCallback.checkInitialized(cache);

    assertThat(jdbcCallback.getSqlHandler()).isNotNull();
  }

  @Test
  public void verifyLoadsAreIgnored() {
    boolean ignoreEvent = jdbcCallback.eventCanBeIgnored(Operation.LOCAL_LOAD_CREATE);
    assertThat(ignoreEvent).isTrue();
  }

  @Test
  public void verifyCreateAreNotIgnored() {
    boolean ignoreEvent = jdbcCallback.eventCanBeIgnored(Operation.CREATE);
    assertThat(ignoreEvent).isFalse();
  }
}
