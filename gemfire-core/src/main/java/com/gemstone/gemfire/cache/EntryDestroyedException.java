/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;
/**
 * Indicates that a method was invoked on an entry that has been destroyed.
 *
 * @author Eric Zoerner
 *
 * 
 * @see Region.Entry
 * @since 3.0
 */
public class EntryDestroyedException extends CacheRuntimeException
{
  private static final long serialVersionUID = 831865939772672542L;
  /** Constructs a new <code>EntryDestroyedException</code>. */ 
  public EntryDestroyedException()
  {
     super();
  }

  /**
   * Constructs a new <code>EntryDestroyedException</code> with the message.
   * @param s the detailed message for this exception
   */
  public EntryDestroyedException(String s)
  {
    super(s);
  }

  /** Constructs a new <code>EntryDestroyedException</code> with a detailed message
   * and a causal exception.
   * @param s the message
   * @param ex a causal Throwable
   */
  public EntryDestroyedException(String s, Throwable ex)
  {
    super(s, ex);
  }
  
  /** Construct a <code>EntryDestroyedException</code> with a cause.
   * @param ex the causal Throwable
   */  
  public EntryDestroyedException(Throwable ex) {
    super(ex);
  }
}
