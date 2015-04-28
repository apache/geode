/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.size;

/**
 * A Size of util class which does nothing. This is useful for running the test
 * with jprobe, because jprobe doesn't play nicely with the -javaagent flag. If
 * we implement a 1.4 SizeOfUtil class, then we probably don't need this one.
 * 
 * @author dsmith
 * 
 */
public class SizeOfUtil0 implements SingleObjectSizer {

  public long sizeof(Object object) {
    return 2;
  }

}
