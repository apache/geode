/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author dsmith
 *
 */
public class TestUtils {
  public static <T> Set<T> asSet(T ... objects) {
    return new HashSet<T> (Arrays.asList(objects));
  }

}
