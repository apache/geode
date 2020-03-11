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
package org.apache.geode.internal.jta;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

/**
 * <p>
 * XidImpl: A JTA compatible implementation of Xid
 * </p>
 *
 * @since GemFire 4.0
 *
 * @deprecated as of Geode 1.2.0 user should use a third party JTA transaction manager to manage JTA
 *             transactions.
 */
@Deprecated
public class XidImpl implements Xid {

  /**
   * The format id will be a constant
   */
  private int formatId;
  /**
   * This will be the global transaction identifier;
   */
  protected byte gtrid[];
  /**
   * bqual will be a constant since we are only supporting one resource manager presently
   */
  private byte bqual[];

  /**
   * Construct a new XidImpl //Asif .: Constructor is made private
   */
  private XidImpl(int formatId, byte gtrid[], byte bqual[]) {
    this.formatId = formatId;
    this.gtrid = gtrid;
    this.bqual = bqual;
  }

  /**
   * Returns the FormatId
   *
   * @see javax.transaction.xa.Xid#getFormatId()
   */
  @Override
  public int getFormatId() {
    return formatId;
  }

  /**
   * Returns the BranchQualifier
   *
   * @see javax.transaction.xa.Xid#getBranchQualifier()
   */
  @Override
  public byte[] getBranchQualifier() {
    return bqual;
  }

  /**
   * Returns the GlobalTransactionId
   *
   * @see javax.transaction.xa.Xid#getGlobalTransactionId()
   */
  @Override
  public byte[] getGlobalTransactionId() {
    return gtrid;
  }

  /**
   * A function to create a new Xid.
   */
  public static Xid createXid(byte[] GTid) throws XAException {
    byte[] globalID = new byte[GTid.length];
    byte[] branchID = new byte[1];
    // we are supporting only one RM So the branch ID is a constant
    branchID[0] = (byte) 1;
    System.arraycopy(GTid, 0, globalID, 0, GTid.length);
    Xid xid = new XidImpl(0x1234, globalID, branchID);
    return xid;
  }
}
