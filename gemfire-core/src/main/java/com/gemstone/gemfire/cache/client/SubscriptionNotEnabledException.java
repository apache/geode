/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client;

import com.gemstone.gemfire.cache.Region;


/**
 * An exception indicating that client subscriptions are not enabled on this client, but
 * the client is trying to perform an operation that requires a client subscription,
 *  such as {@link Region#registerInterest(Object)}.
 * @author dsmith
 * @since 5.7
 */
public class SubscriptionNotEnabledException extends ServerConnectivityException {
  private static final long serialVersionUID = -8212446737778234890L;

  /**
   * Create a new instance of SubscriptionNotEnabledException without a detail message or cause.
   */
  public SubscriptionNotEnabledException() {
  }

  /**
   * Create a new instance of SubscriptionNotEnabledException with a detail message
   * @param message the detail message
   */
  public SubscriptionNotEnabledException(String message) {
    super(message);
  }

  /**
   * Create a new instance of SubscriptionNotEnabledException with a detail message and cause
   * @param message the detail message
   * @param cause the cause
   */
  public SubscriptionNotEnabledException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Create a new instance of SubscriptionNotEnabledException with a and cause
   * @param cause the cause
   */
  public SubscriptionNotEnabledException(Throwable cause) {
    super(cause);
  }

}
