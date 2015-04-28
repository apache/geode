/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util;

/**
 * Represents a data transform between two types.
 * @author rholmes
 *
 * @param <T1> The data type to be transformed from.
 * @param <T2> The data type to be transformed to.
 */
public interface Transformer<T1,T2> {
  /**
   * Transforms one data type into another.
   * @param t the data to be transferred from.
   * @return the transformed data.
   */
  public T2 transform(T1 t);
}
