/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal;

import java.io.NotSerializableException;

/**
 * Exception to indicate that a specified DSFID type could not be found (e.g. due to class being
 * absent in lower product versions).
 */
public class DSFIDNotFoundException extends NotSerializableException {

  private static final long serialVersionUID = 130596009484324655L;

  private int dsfid;
  private short versionOrdinal;

  /**
   * Constructs a DSFIDNotFoundException object with message string.
   * 
   * @param msg exception message
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
