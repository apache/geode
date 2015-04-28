/*
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 */

package com.gemstone.gemfire.internal.lang;

/**
 * The Filter interface defines a contract for implementing objects that act as a filter to segregate other objects.
 * </p>
 * @author John Blum
 * @since 7.0
 */
public interface Filter<T> {

  public boolean accept(T obj);

}
