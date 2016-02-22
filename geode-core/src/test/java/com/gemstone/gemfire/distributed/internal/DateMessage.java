/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
