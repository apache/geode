/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.shell.jline;

import jline.UnsupportedTerminal;

/**
 * Used when gfsh is run in Head Less mode. 
 * Doesn't support ANSI.
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
 */
public class GfshUnsupportedTerminal extends UnsupportedTerminal {
  @Override
  public boolean isANSISupported() {
    return false;
  }
}
