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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.Operation;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class PreparedStatementCacheTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private PreparedStatementCache cache;
  private Connection connection;
  private List<ColumnValue> values = new ArrayList<>();

  @Before
  public void setup() throws SQLException {
    cache = new PreparedStatementCache();
    connection = mock(Connection.class);
    when(connection.prepareStatement(any())).thenReturn(mock(PreparedStatement.class));
    values.add(mock(ColumnValue.class));
  }

  @Test
  public void returnsSameStatementForIdenticalInputs() throws Exception {
    cache.getPreparedStatement(connection, values, "table1", Operation.UPDATE, 1);
    cache.getPreparedStatement(connection, values, "table1", Operation.UPDATE, 1);
    verify(connection, times(1)).prepareStatement(any());
  }

  @Test
  public void returnsDifferentStatementForNonIdenticalInputs() throws Exception {
    cache.getPreparedStatement(connection, values, "table1", Operation.UPDATE, 1);
    cache.getPreparedStatement(connection, values, "table2", Operation.UPDATE, 1);
    verify(connection, times(2)).prepareStatement(any());
  }

  @Test()
  public void throwsExceptionIfPreparingStatementFails() throws Exception {
    when(connection.prepareStatement(any())).thenThrow(SQLException.class);
    thrown.expect(IllegalStateException.class);
    cache.getPreparedStatement(connection, values, "table1", Operation.UPDATE, 1);
  }

  @Test
  public void throwsExceptionIfInvalidOperationGiven() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    cache.getPreparedStatement(connection, values, "table1", Operation.REGION_CLOSE, 1);

  }
}
