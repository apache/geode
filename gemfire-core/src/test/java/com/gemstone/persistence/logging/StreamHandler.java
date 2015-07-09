/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.persistence.logging;

import java.io.*;

/**
 * A <code>StreamHandler</code> exports log records to an
 * <code>OutputStream</code>. 
 */
public class StreamHandler extends Handler {

  /** The destination PrintWriter */
  private PrintWriter pw;

  /**
   * Creates a new <code>StreamHandler</code> that exports log records
   * to an <code>OutputStream</code> in a given format.
   */
  public StreamHandler(OutputStream stream, Formatter formatter) {
    super();
    this.pw = new PrintWriter(stream, true);
    this.setFormatter(formatter);
  }

  public void close() {
    this.pw.close();
  }

  public void flush() {
    this.pw.flush();
  }

  public boolean isLoggable(LogRecord record) {
    if(this.pw == null) {
      return(false);
    } else {
      return(super.isLoggable(record));
    }
  }

  public void publish(LogRecord record) {
    Formatter formatter = this.getFormatter();
    pw.print(formatter.format(record));
  }

}
