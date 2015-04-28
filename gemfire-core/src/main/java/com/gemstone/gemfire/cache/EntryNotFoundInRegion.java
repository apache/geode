/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.InternalGemFireError;

/**
 * @deprecated this class is no longer in use
 */
@Deprecated
public class EntryNotFoundInRegion extends GemFireException {
  private static final long serialVersionUID = 5572550909947420405L;

  /**
   * Generates an {@link InternalGemFireError}
   * @param msg the detail message
   * @deprecated Do not create instances of this class.
   */
  @Deprecated
  public EntryNotFoundInRegion(String msg) {
    throw new InternalGemFireError(LocalizedStrings.EntryNotFoundInRegion_THIS_CLASS_IS_DEPRECATED.toLocalizedString());
  }

  /**
   * Generates an {@link InternalGemFireError}
   * @param msg the detail message
   * @param cause the causal Throwable
   * @deprecated do not create instances of this class.
   */
  @Deprecated
  public EntryNotFoundInRegion(String msg, Throwable cause) {
    throw new InternalGemFireError(LocalizedStrings.EntryNotFoundInRegion_THIS_CLASS_IS_DEPRECATED.toLocalizedString());
  }
}
