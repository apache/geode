/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.hibernate;

import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

public class GemFireCacheListener extends CacheListenerAdapter implements
    Declarable {

  @Override
  public void afterCreate(EntryEvent event) {
    System.out.println("Create : " + event.getKey() + " / "
        + event.getNewValue());
  }

  @Override
  public void afterDestroy(EntryEvent event) {
    System.out.println("Destroy : " + event.getKey());
  }

  @Override
  public void afterInvalidate(EntryEvent event) {
    System.out.println("Invalidate : " + event.getKey());
  }

  @Override
  public void afterUpdate(EntryEvent event) {
    System.out.println("Update : " + event.getKey() + " / "
        + event.getNewValue());
  }

  public void init(Properties props) {

  }

}
