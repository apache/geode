/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.catalina.callback;

import java.util.Properties;

import org.apache.catalina.Session;

import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import javax.servlet.http.HttpSession;

public class LocalSessionCacheWriter extends
    CacheWriterAdapter<String, HttpSession> implements Declarable {

  private final Region<String,HttpSession> backingRegion;
  
  public LocalSessionCacheWriter(Region<String,HttpSession> backingRegion) {
    this.backingRegion = backingRegion;
  }

  public void beforeCreate(EntryEvent<String,HttpSession> event) throws CacheWriterException {
    this.backingRegion.put(event.getKey(), event.getNewValue(), event.getCallbackArgument());
  }

  public void beforeUpdate(EntryEvent<String,HttpSession> event) throws CacheWriterException {
    this.backingRegion.put(event.getKey(), event.getNewValue(), event.getCallbackArgument());
  }

  public void beforeDestroy(EntryEvent<String,HttpSession> event) throws CacheWriterException {
    try {
      this.backingRegion.destroy(event.getKey(), event.getCallbackArgument());
    } catch (EntryNotFoundException e) {
      // I think it is safe to ignore this exception. The entry could have
      // expired already in the backing region.
    }
  }

  public void close() {}

  public void init(Properties p) {}
}
