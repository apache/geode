/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.jta;

/**
 * <p>
 * XidImpl: A JTA compatible implementation of Xid
 * </p>
 * 
 * @author Mitul Bid
 * @since 4.0 
 */
import javax.transaction.xa.*;

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
   * bqual will be a constant since we are only supporting one resource manager
   * presently
   */
  private byte bqual[];

  /**
   * Construct a new XidImpl
   *  //Asif .: Constructor is made private
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
  public int getFormatId() {
    return formatId;
  }

  /**
   * Returns the BranchQualifier
   * 
   * @see javax.transaction.xa.Xid#getBranchQualifier()
   */
  public byte[] getBranchQualifier() {
    return bqual;
  }

  /**
   * Returns the GlobalTransactionId
   * 
   * @see javax.transaction.xa.Xid#getGlobalTransactionId()
   */
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
    branchID[0] = (byte)1; 
    System.arraycopy(GTid, 0, globalID, 0, GTid.length);
    Xid xid = new XidImpl(0x1234, globalID, branchID);
    return xid;
  }
}
