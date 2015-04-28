/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.jta;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.PureLogWriter;

/**
 * Contains Utility functions for use by JTA
 * 
 * @author Mitul D Bid
 */
public class TransactionUtils {

  private static LogWriterI18n dslogWriter = null;
  private static LogWriterI18n purelogWriter = null;

  /**
   * Returns the logWriter associated with the existing DistributedSystem. If
   * DS is null then the PureLogWriter is returned
   * 
   * @return LogWriterI18n
   */
  public static LogWriterI18n getLogWriterI18n() {
    if (dslogWriter != null) {
      return dslogWriter;
    } else if (purelogWriter != null) {
      return purelogWriter;
    } else {
      purelogWriter = new PureLogWriter(InternalLogWriter.SEVERE_LEVEL);
      return purelogWriter;
    }
  }

  /**
   * To be used by mapTransaction method of JNDIInvoker to set the dsLogwriter
   * before the binding of the datasources
   * 
   * @param logWriter
   */
  public static void setLogWriter(LogWriterI18n logWriter) {
    dslogWriter = logWriter;
  }
}
