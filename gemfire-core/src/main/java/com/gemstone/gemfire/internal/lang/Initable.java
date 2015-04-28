/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */

package com.gemstone.gemfire.internal.lang;

/**
 * The Initable interface defines a contract for implementing classes who's Object instances can be initialized after
 * construction.
 * <p/>
 * @author John Blum
 * @since 6.8
 */
public interface Initable {

  /**
   * Called to perform additional initialization logic after construction of the Object instance.  This is necessary
   * in certain cases to prevent escape of the "this" reference during construction by subclasses needing to
   * instantiate other collaborators or starting of additional services, like Threads, and so on.
   */
  public void init();

}
