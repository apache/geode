/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.rest.internal.web.util;

/**
 * The ValidationUtils class is a utility class for performing validations.
 * <p/>
 * @author John Blum, Nilkanth Patel.
 * @since 8.0
 */

@SuppressWarnings("unused")
public abstract class ValidationUtils {

  public static <T> T returnValueThrowOnNull(final T value, final String message, final Object... args) {
    return returnValueThrowOnNull(value, new NullPointerException(String.format(message, args)));
  }

  public static <T> T returnValueThrowOnNull(final T value, final RuntimeException e) {
    if (value == null) {
      throw e;
    }
    return value;
  }
}

