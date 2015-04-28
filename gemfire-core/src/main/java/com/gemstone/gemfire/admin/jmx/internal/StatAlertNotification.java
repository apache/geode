/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.jmx.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Serializable;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.StatAlert;
import com.gemstone.gemfire.internal.admin.StatAlertDefinition;

/**
 * Notification to be sent to clients (e.g GFMon2.0 ). It incorporates
 * 
 * @see StatAlert raised and also Gemfire member id which raised the alert
 * 
 * @author abhishek
 * 
 * @since 5.7
 */
public class StatAlertNotification extends StatAlert implements Serializable, DataSerializable, DataSerializableFixedID {
  private static final long serialVersionUID = -1634729103430107871L;
  private String memberId;

  public StatAlertNotification() {
  }

  public StatAlertNotification(StatAlert statAlert, String memberId) {
    this.setDefinitionId(statAlert.getDefinitionId());
    this.setValues(statAlert.getValues());
    this.setTime(statAlert.getTime());
    this.memberId = memberId;
  }

  public int getDSFID() {
    return DataSerializableFixedID.STAT_ALERT_NOTIFICATION;
  }

  /**
   * @return the memberId
   */
  public String getMemberId() {
    return memberId;
  }

  /**
   * 
   * @param id
   *                of gemfire member which raised the alert
   */
  public void setMemberId(String id) {
    memberId = id;
  }

  /**
   * @return String representation of this object
   */
  @Override
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("[");
    for (int i = 0; i < getValues().length; i++) {
      buf.append(getValues()[i] + ", ");
    }
    buf.append("]");
    return Integer.valueOf(getDefinitionId()) + ":" + buf.toString();
  }

  /**
   * The notification is something like this
   * "For Member ID: <ID>
   * [
   *  <StatName> = <Value>
   *  .. 
   * ]"
   * @param defn
   *                {@link StatAlertDefinition}
   * @return String representation of this object based on
   *         {@link StatAlertDefinition}
   */
  public String toString(StatAlertDefinition defn) {
    StringBuffer buf = new StringBuffer();
    buf.append("For Member ID: ");
    buf.append(this.memberId);
    buf.append("\n");
    buf.append("[ ");
    for (int i = 0; i < getValues().length; i++) {
      buf.append(defn.getStatisticInfo()[i].toString() + "=" + getValues()[i]
          + "\n");
    }
    buf.append("]");
    return getTime().toString() + ":" + buf.toString();
  }

  @Override
  public boolean equals(Object object) {
    if (object != null && !(object instanceof StatAlertNotification)) {
      return false;
    }

    StatAlertNotification other = (StatAlertNotification)object;

    int defId = getDefinitionId();

    if (defId != -1 && defId == other.getDefinitionId() && memberId != null
        && memberId.equals(other.getMemberId())) {
      return true;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return memberId.hashCode();
  }

  public void toData(DataOutput out) throws IOException {
    // Do not modify StatAlert to allow 57 cacheservers to function with 57+ agent
    // However, update of a new StatAlertDefn on 57 server from 57+ agent not covered with this
    DataSerializer.writePrimitiveInt(this.getDefinitionId(), out);
    DataSerializer.writeDate(this.getTime(), out);
    DataSerializer.writeObjectArray(this.getValues(), out);

    DataSerializer.writeString(this.memberId, out);
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    // Do not modify StatAlert to allow 57 cacheservers to function with 57+ agent
    // However, update of a new StatAlertDefn on 57 server from 57+ agent not covered with this
    this.setDefinitionId(DataSerializer.readPrimitiveInt(in));
    this.setTime(DataSerializer.readDate(in));
    this.setValues((Number[])DataSerializer.readObjectArray(in));

    this.memberId = DataSerializer.readString(in);
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
