/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.CacheCallback;

public interface InterestFilter extends CacheCallback {

  public boolean notifyOnCreate(InterestEvent event);

  public boolean notifyOnUpdate(InterestEvent event);

  public boolean notifyOnDestroy(InterestEvent event);

  public boolean notifyOnInvalidate(InterestEvent event);

  public boolean notifyOnRegister(InterestEvent event);


}
