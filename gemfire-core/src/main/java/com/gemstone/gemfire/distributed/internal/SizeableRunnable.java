/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;


/** SizeableRunnable is a Runnable with a size.  It implements the Sizeable interface
    for use in QueuedExecutors using a ThrottlingMemLinkedBlockingQueue.<p>
    
    Instances/subclasses must provide the run() method to complete the
    Runnable interface.<p>
    
    @author Bruce Schuchardt
    @since 5.0
 */

public abstract class SizeableRunnable implements Runnable, Sizeable {
  private int size;
  
  public SizeableRunnable(int size) {
    this.size = size;
  }
  public int getSize() {
    return this.size;
  }
}
