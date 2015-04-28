/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog.io;

import java.util.regex.Pattern;

import com.gemstone.gemfire.internal.sequencelog.GraphType;

/**
 * @author dsmith
 *
 */
public interface Filter {

  public boolean accept(GraphType graphType, String name, String edgeName, String source, String dest);
  public boolean acceptPattern(GraphType graphType, Pattern pattern, String edgeName, String source, String dest);

}
