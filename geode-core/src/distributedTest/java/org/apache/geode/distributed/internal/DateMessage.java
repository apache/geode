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
package org.apache.geode.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * This message simply contains a date
 */
public class DateMessage extends SerialDistributionMessage {

  /** Formats a data */
  private static final DateFormat format = new SimpleDateFormat("M/dd/yyyy hh:mm:ss.SSS");

  /** The date being distributed */
  private Date date;
  /** The versions in which this message was modified */
  private static final KnownVersion[] dsfidVersions = new KnownVersion[] {};

  ///////////////////// Instance Methods /////////////////////

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
    return date;
  }

  /**
   * Just prints out the date
   */
  @Override
  public void process(ClusterDistributionManager dm) {
    // Make sure that message state is what we expect
    Assert.assertTrue(date != null);

    System.out.println(format.format(date));
  }

  @Override
  public void reset() {
    date = null;
  }

  ////////////////// Externalizable Methods //////////////////

  @Override
  public int getDSFID() {
    return NO_FIXED_ID;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(date, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {

    super.fromData(in, context);
    date = DataSerializer.readObject(in);
  }

  public String toString() {
    return format.format(date);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return dsfidVersions;
  }

}
