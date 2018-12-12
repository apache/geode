/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.jndi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;
import javax.sql.DataSource;

import org.junit.Test;

import org.apache.geode.internal.datasource.DataSourceCreateException;
import org.apache.geode.internal.datasource.DataSourceFactory;

public class JNDIInvokerTest {

  @Test
  public void verifyThatMapDataSourceWithGetSimpleDataSourceThrowingWillThrowThatException()
      throws Exception {
    Map<String, String> inputs = new HashMap<>();
    Context context = mock(Context.class);
    DataSourceFactory dataSourceFactory = mock(DataSourceFactory.class);
    DataSourceCreateException exception = mock(DataSourceCreateException.class);
    doThrow(exception).when(dataSourceFactory).getSimpleDataSource(any());
    inputs.put("type", "SimpleDataSource");

    Throwable thrown =
        catchThrowable(() -> JNDIInvoker.mapDatasource(inputs, null, dataSourceFactory, context));

    assertThat(thrown).isSameAs(exception);
  }

  @Test
  public void mapDataSourceWithGetConnectionExceptionThrowsDataSourceCreateException()
      throws Exception {
    Map<String, String> inputs = new HashMap<>();
    Context context = mock(Context.class);
    DataSource dataSource = mock(DataSource.class);
    SQLException exception = mock(SQLException.class);
    doThrow(exception).when(dataSource).getConnection();
    DataSourceFactory dataSourceFactory = mock(DataSourceFactory.class);
    when(dataSourceFactory.getSimpleDataSource(any())).thenReturn(dataSource);
    inputs.put("type", "SimpleDataSource");
    String jndiName = "myJndiBinding";
    inputs.put("jndi-name", jndiName);

    Throwable thrown =
        catchThrowable(() -> JNDIInvoker.mapDatasource(inputs, null, dataSourceFactory, context));

    assertThat(thrown).isInstanceOf(DataSourceCreateException.class)
        .hasMessage("Failed to connect to \"" + jndiName + "\". See log for details");
    verify(context, never()).rebind((String) any(), any());
  }

  @Test
  public void mapDataSourceWithGetConnectionSuccessClosesConnectionAndRebinds() throws Exception {
    Map<String, String> inputs = new HashMap<>();
    Context context = mock(Context.class);
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    when(dataSource.getConnection()).thenReturn(connection);
    DataSourceFactory dataSourceFactory = mock(DataSourceFactory.class);
    when(dataSourceFactory.getSimpleDataSource(any())).thenReturn(dataSource);
    inputs.put("type", "SimpleDataSource");
    String jndiName = "myJndiBinding";
    inputs.put("jndi-name", jndiName);

    JNDIInvoker.mapDatasource(inputs, null, dataSourceFactory, context);

    verify(connection).close();
    verify(context).rebind((String) any(), same(dataSource));
  }
}
