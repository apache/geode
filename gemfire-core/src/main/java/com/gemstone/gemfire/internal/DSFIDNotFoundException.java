/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal;

import java.io.NotSerializableException;

/**
 * Exception to indicate that a specified DSFID type could not be found (e.g.
 * due to class being absent in lower product versions).
 */
public class DSFIDNotFoundException extends NotSerializableException {

  private static final long serialVersionUID = 130596009484324655L;

  private int dsfid;
  private short versionOrdinal;

  /**
   * Constructs a DSFIDNotFoundException object with message string.
   * 
   * @param msg
   *          exception message
   */
  public DSFIDNotFoundException(String msg, int dsfid) {
    super(msg);
    this.dsfid = dsfid;
    this.versionOrdinal = Version.CURRENT.ordinal();
  }

  public int getUnknownDSFID() {
    return this.dsfid;
  }

  public short getProductVersionOrdinal() {
    return this.versionOrdinal;
  }
}
