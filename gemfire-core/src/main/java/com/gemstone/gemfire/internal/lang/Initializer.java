/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.lang;

/**
 * The Initializer class is a utility class to identify Initable objects and initialize them by calling their
 * init method.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.internal.lang.Initable
 * @since 8.0
 */
public class Initializer {

  /**
   * Initializes the specified Object by calling it's init method if and only if the Object implements the
   * Initable interface.
   * <p/>
   * @param initableObj the Object targeted to be initialized.
   * @return true if the target Object was initialized using an init method; false otherwise.
   * @see com.gemstone.gemfire.internal.lang.Initable#init()
   */
  public static boolean init(final Object initableObj) {
    if (initableObj instanceof Initable) {
      ((Initable) initableObj).init();
      return true;
    }

    return false;
  }

}
