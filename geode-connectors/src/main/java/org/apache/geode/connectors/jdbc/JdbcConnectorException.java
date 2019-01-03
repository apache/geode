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

import java.sql.SQLException;

import org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.geode.cache.CacheRuntimeException;

/**
 * An exception thrown when communication with an external JDBC data source fails and can be used
 * to diagnose the cause of database communication failures. In cases where the cause of this
 * exception is not safe to serialize to clients, the stack trace is included in the message of the
 * exception and the cause is left empty.
 *
 * @since Geode 1.5
 */
public class JdbcConnectorException extends CacheRuntimeException {
  private static final long serialVersionUID = 1L;

  /**
   * Create a new JdbcConnectorException by first checking to see if the causing exception is or
   * contains an exception that potentially could not be deserialized by remote systems receiving
   * the serialized exception.
   *
   * @param e cause of this Exception
   * @return a new JdbcConnectorException containing either the causing exception, if it can be
   *         serialized/deserialized by Geode, or containing the causing exception stack trace in
   *         its message if not
   */
  public static JdbcConnectorException createException(Exception e) {
    if (containsNonSerializableException(e)) {
      String message =
          e.getMessage() + System.lineSeparator() + ExceptionUtils.getStackTrace(e);
      return new JdbcConnectorException(message);
    } else {
      return new JdbcConnectorException(e);
    }
  }

  /**
   * Create a new JdbcConnectorException by first checking to see if the causing exception is or
   * contains an exception that potentially could not be deserialized by remote systems receiving
   * the serialized exception.
   *
   * @param message message of this Exception
   * @param e cause of this Exception
   * @return a new JdbcConnectorException containing either the causing exception, if it can be
   *         serialized/deserialized by Geode, or containing the causing exception stack trace in
   *         its message if not
   */
  public static JdbcConnectorException createException(String message, Exception e) {
    if (containsNonSerializableException(e)) {
      message += e.getMessage() + System.lineSeparator() + ExceptionUtils.getStackTrace(e);
      return new JdbcConnectorException(message);
    } else {
      return new JdbcConnectorException(message, e);
    }
  }

  public JdbcConnectorException(String message) {
    super(message);
  }

  /*
   * SQLExceptions likely are instances of or contain exceptions from the underlying SQL driver
   * and potentially cannot be deserialzed by other systems (e.g. client or locators) that do not
   * possess the driver on their classpaths.
   */
  private static boolean containsNonSerializableException(Exception e) {
    if (e == null) {
      return false;
    }

    if (e instanceof SQLException) {
      return true;
    }

    Throwable cause = e.getCause();
    while (cause != null) {
      if (cause instanceof SQLException) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }

  private JdbcConnectorException(Exception e) {
    super(e);
  }

  private JdbcConnectorException(String message, Exception e) {
    super(message, e);
  }
}
