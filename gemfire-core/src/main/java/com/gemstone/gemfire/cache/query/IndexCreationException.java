/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query;
/**
 * This class is used to represent any partitioned index creation exceptions.
 * 
 * @author rdubey
 */
public class IndexCreationException extends QueryException
{
  private static final long serialVersionUID = -2218359458870240534L;

  /**
   * Constructor with a string message representing the problem.
   * 
   * @param msg message representing the cause of exception
   */
  public IndexCreationException(String msg) {
    super(msg);
  }
  
  /**
   * Constructor with a string message representing the problem and also the 
   * throwable.
   * @param msg representing the cause of exception
   * @param cause the actual exception.
   */
  public IndexCreationException(String msg, Throwable cause) {
    super(msg, cause);
  }

}
