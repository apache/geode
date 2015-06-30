/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.company.app;

import com.gemstone.gemfire.cache.*;

/**
 * com.company.app.OrdersCacheListener. Cache listener impl for CacheXmlxxTest
 *
 * @author Kirk Lund
 * @since 5.0
 */
public class OrdersCacheListener implements CacheListener, Declarable {

  public OrdersCacheListener() {}

  public void afterCreate(EntryEvent event) {}

  public void afterUpdate(EntryEvent event) {}

  public void afterInvalidate(EntryEvent event) {}

  public void afterDestroy(EntryEvent event) {}

  public void afterRegionInvalidate(RegionEvent event) {}

  public void afterRegionDestroy(RegionEvent event) {}

  public void afterRegionClear(RegionEvent event) {}
  
  public void afterRegionCreate(RegionEvent event) {}
  
  public void afterRegionLive(RegionEvent event) {}
  
  public void close() {}
  
  public void init(java.util.Properties props) {}
}
