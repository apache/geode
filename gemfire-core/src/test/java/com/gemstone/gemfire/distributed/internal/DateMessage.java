/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.Version;

import java.io.*;
import java.text.*;
import java.util.*;

/**
 * This message simply contains a date
 */
public class DateMessage extends SerialDistributionMessage {
  
  /** Formats a data */
  private static final DateFormat format = 
    new SimpleDateFormat("M/dd/yyyy hh:mm:ss.SSS");

  /** The date being distributed */
  private Date date;
  /** The versions in which this message was modified */
  private static final Version[] dsfidVersions = new Version[]{};

  /////////////////////  Instance Methods  /////////////////////

  /**
   * Sets the date associated with this <code>DateMessage</code>
   */
  public void setDate(Date date) {
    this.date = date;
  }

  /**
   * Returns the date associated with this message.
   */
  public Date getDate() {
    return this.date;
  }

  /**
   * Just prints out the date
   */
  public void process(DistributionManager dm) {
    // Make sure that message state is what we expect
    Assert.assertTrue(this.date != null);

    System.out.println(format.format(this.date));
  }

  public void reset() {
    this.date = null;
  }

  //////////////////  Externalizable Methods  //////////////////

  public int getDSFID() {
    return NO_FIXED_ID;
  }

  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.date, out);
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {

    super.fromData(in);
    this.date = (Date) DataSerializer.readObject(in);
  }

  public String toString() {
    return format.format(this.date);
  }

  @Override
  public Version[] getSerializationVersions() {
    return dsfidVersions;
  }

}
