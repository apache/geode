/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.io;

import java.io.PrintStream;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public class TeePrintStream extends PrintStream {

  private final TeeOutputStream teeOut;
  
  public TeePrintStream(TeeOutputStream teeOut) {
    super(teeOut, true);
    this.teeOut = teeOut;
  }
  
  public TeeOutputStream getTeeOutputStream() {
    return this.teeOut;
  }
  
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("teeOutputStream=").append(this.teeOut);
    return sb.append("}").toString();
  }
}
