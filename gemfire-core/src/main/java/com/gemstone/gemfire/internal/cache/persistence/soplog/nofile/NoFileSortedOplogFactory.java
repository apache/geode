/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog.nofile;

import java.io.File;
import java.io.IOException;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogFactory;

public class NoFileSortedOplogFactory implements SortedOplogFactory {
  private final SortedOplogConfiguration config;
  
  public NoFileSortedOplogFactory(String name) {
    config = new SortedOplogConfiguration(name);
  }
  
  @Override
  public SortedOplog createSortedOplog(File name) throws IOException {
    return new NoFileSortedOplog(config);
  }

  @Override
  public SortedOplogConfiguration getConfiguration() {
    return config;
  }
}
