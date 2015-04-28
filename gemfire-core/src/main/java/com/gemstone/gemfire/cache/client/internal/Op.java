/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

/**
 * An operation to perform on a server. Used by
 * {@link ExecutablePool} to attempt the operation on 
 * multiple servers until the retryAttempts is exceeded.
 * @author dsmith
 * @since 5.7
 *
 */
public interface Op {

  /**
   * Attempts to execute this operation by sending its message out on the
   * given connection, waiting for a response, and returning it.
   * @param cnx the connection to use when attempting execution of the operation.
   * @return the result of the operation
   *         or <code>null</code if the operation has no result.
   * @throws Exception if the execute failed
   */
  Object attempt(Connection cnx) throws Exception;

  /**
   * @return true if this Op should use a threadLocalConnection, false otherwise
   */
  boolean useThreadLocalConnection();
}