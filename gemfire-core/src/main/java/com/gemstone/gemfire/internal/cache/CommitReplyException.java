/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.distributed.internal.ReplyException;
import java.util.*;

/**
 * Contains exceptions generated when attempting to process a commit operation.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public class CommitReplyException extends ReplyException {
private static final long serialVersionUID = -7711083075296622596L;
  
  /** Exceptions generated when attempting to process a commit operation */
  private final Set exceptions;
  
  /** 
   * Constructs a <code>CommitReplyException</code> with a message.
   *
   * @param s the String message
   */
  public CommitReplyException(String s) {
    super(s);
    this.exceptions = Collections.EMPTY_SET;
  }
  
  /** 
   * Constructs a <code>CommitReplyException</code> with a message and
   * set of exceptions generated when attempting to process a commit operation.
   *
   * @param s the String message
   * @param exceptions set of exceptions generated when attempting to process
   * a commit operation
   */
  public CommitReplyException(String s, Set exceptions) {
    super(s);
    this.exceptions = exceptions;
  }
  
  /** 
   * Returns set of exceptions generated when attempting to process a 
   * commit operation
   *
   * @return set of exceptions generated when attempting to process a 
   * commit operation
   */
  public Set getExceptions() {
    return this.exceptions;
  }

  @Override
  public String toString() {
    return super.toString() + " with exceptions: " + this.exceptions;
  }
}

