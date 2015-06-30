/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

public class QueueListener implements CacheListener{
  public List createList = Collections.synchronizedList(new ArrayList());
  public List destroyList = Collections.synchronizedList(new ArrayList());
  public List updateList = Collections.synchronizedList(new ArrayList());
  
  public void afterCreate(EntryEvent event) {
    createList.add(event.getKey());
  }

  public void afterDestroy(EntryEvent event) {
    destroyList.add(event.getKey());
  }

  public void afterInvalidate(EntryEvent event) {
    // TODO Auto-generated method stub
    
  }

  public void afterRegionClear(RegionEvent event) {
    // TODO Auto-generated method stub
    
  }

  public void afterRegionCreate(RegionEvent event) {
    // TODO Auto-generated method stub
    
  }

  public void afterRegionDestroy(RegionEvent event) {
    // TODO Auto-generated method stub
    
  }

  public void afterRegionInvalidate(RegionEvent event) {
    // TODO Auto-generated method stub
    
  }

  public void afterRegionLive(RegionEvent event) {
    // TODO Auto-generated method stub
    
  }

  public void afterUpdate(EntryEvent event) {
    updateList.add(event.getKey());
  }

  public void close() {
    // TODO Auto-generated method stub
    
  }
  
}