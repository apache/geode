/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.sequencelog.model;

import java.util.regex.Pattern;

import com.gemstone.gemfire.internal.sequencelog.GraphType;

/**
 * @author dsmith
 *
 */
public interface GraphReaderCallback {

  public abstract void addEdge(long timestamp, GraphType graphType,
      String graphName, String edgeName, String state, String source,
      String dest);
  
  public abstract void addEdgePattern(long timestamp, GraphType graphType,
      Pattern graphNamePattern, String edgeName, String state, String source,
      String dest);

}