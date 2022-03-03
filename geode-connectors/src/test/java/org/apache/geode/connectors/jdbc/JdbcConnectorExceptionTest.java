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

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;


public class JdbcConnectorExceptionTest {
  @Rule
  public TestName testName = new TestName();

  @Test
  public void returnsExceptionWithCauseForNonSqlException() {
    Exception e = JdbcConnectorException.createException(new IllegalStateException());
    assertThat(e.getCause()).isNotNull().isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void returnsExceptionWithCauseForNonSqlExceptionAndNonSqlNestedCause() {
    IllegalStateException cause = new IllegalStateException(new IllegalStateException());
    Exception e = JdbcConnectorException.createException(cause);
    assertThat(e.getCause()).isNotNull().isSameAs(cause);
  }

  @Test
  public void returnsExceptionWithCauseForNonSqlExceptionWithMessage() {
    Exception e = JdbcConnectorException.createException("message", new IllegalStateException());
    assertThat(e.getMessage()).isEqualTo("message");
    assertThat(e.getCause()).isNotNull().isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void returnsExceptionWithNoCauseForSqlException() {
    Exception sqlException = new SQLException("mySqlExceptionMessage");
    Exception e = JdbcConnectorException.createException(sqlException);
    assertThat(e.getCause()).isNull();
    assertThat(e.getMessage()).contains("mySqlExceptionMessage")
        .contains(getClass().getCanonicalName() + "." + testName.getMethodName());
  }

  @Test
  public void returnsExceptionWithNoCauseForSqlExceptionWithMessage() {
    Exception sqlException = new SQLException();
    Exception e = JdbcConnectorException.createException("message", sqlException);
    assertThat(e.getCause()).isNull();
    assertThat(e.getMessage()).startsWith("message")
        .contains(getClass().getCanonicalName() + "." + testName.getMethodName());
  }

  @Test
  public void returnsExceptionWithNoCauseForNestedSqlException() {
    Exception sqlException = new SQLException();
    Exception e = JdbcConnectorException.createException(new IllegalStateException(sqlException));
    assertThat(e.getMessage())
        .contains(getClass().getCanonicalName() + "." + testName.getMethodName())
        .contains("SQLException").contains("IllegalStateException");
  }

  @Test
  public void returnsExceptionForNull() {
    Exception e = JdbcConnectorException.createException(null);
    assertThat(e.getCause()).isNull();
  }
}
