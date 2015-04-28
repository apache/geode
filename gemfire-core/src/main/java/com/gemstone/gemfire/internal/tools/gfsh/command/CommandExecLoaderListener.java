/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.tools.gfsh.command;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.SerializedCacheValue;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.internal.tools.gfsh.command.AbstractCommandTask;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandTask;

/**
 * 
 * @author abhishek
 */
@SuppressWarnings("rawtypes")
public class CommandExecLoaderListener 
  extends CacheWriterAdapter implements CacheLoader, CacheListener {
  
  public CommandExecLoaderListener() {}

  public Object load(LoaderHelper helper) throws CacheLoaderException {    
    Object retval = null;
    CommandTask task = (CommandTask) helper.getArgument();
    if (task != null) {
      if (task instanceof AbstractCommandTask) {
        ((AbstractCommandTask)task).setCommandRegion(helper.getRegion());
      }
      retval = task.runTask(null);
      
      if (helper.getKey().toString().startsWith("_bcast")) {
        Region broadcastRegion = helper.getRegion().getRegionService().getRegion("/__command/broadcast");
        broadcastRegion.put(helper.getKey(), task);
      }
    }
    return retval;
  }
  
  private void executeForWrite(EntryEvent event) {
    CommandTask task = (CommandTask) event.getKey();
    SerializedCacheValue obj = event.getSerializedNewValue();
    if (task != null) {
      task.runTask(obj);
    }
  }

  public void beforeCreate(EntryEvent event) throws CacheWriterException {
    executeForWrite(event);
  }

  public void beforeUpdate(EntryEvent event) throws CacheWriterException {
    executeForWrite(event);
  }
  
  private void executeForListener(EntryEvent event) {
    CommandTask task = (CommandTask) event.getNewValue();
    if (task != null) {
      task.runTask(task);
    }
  }

  public void afterCreate(EntryEvent event) {
    executeForListener(event);
  }

  public void afterUpdate(EntryEvent event) {
    executeForListener(event);
  }

  public void afterInvalidate(EntryEvent event) {}

  public void afterDestroy(EntryEvent event) {}

  public void afterRegionInvalidate(RegionEvent event) {}

  public void afterRegionDestroy(RegionEvent event) {}

  public void afterRegionClear(RegionEvent event) {}

  public void afterRegionCreate(RegionEvent event) {}

  public void afterRegionLive(RegionEvent event) {}

  public void close() {}
}
