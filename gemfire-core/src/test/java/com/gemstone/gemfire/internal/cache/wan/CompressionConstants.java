/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.wan;

public interface CompressionConstants {

  /** Constants for 6-bit code values. */

  /** No operation: used to pad words on flush. */
  static final int NOP     = 0;  

  /** Introduces raw byte format. */
  static final int RAW     = 1;  

  /** Format indicator for characters found in lookup table. */
  static final int BASE    = 2;  

  /** A character's code is it's index in the lookup table. */
  static final String codeTable =
      "abcdefghijklmnopqrstuvwxyz" +
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ ,.!?\"'()";
  
}
