/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.catalina.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.modules.session.catalina.DeltaSession;

@SuppressWarnings("serial")
public class DeltaSessionUpdateAttributeEvent implements DeltaSessionAttributeEvent {

  private String attributeName;

  private Object attributeValue;
  
  public DeltaSessionUpdateAttributeEvent() {
  }

  public DeltaSessionUpdateAttributeEvent(String attributeName, Object attributeValue) {
    this.attributeName = attributeName;
    this.attributeValue = attributeValue;
  }
  
  public String getAttributeName() {
    return this.attributeName;
  }

  public Object getAttributeValue() {
    return this.attributeValue;
  }

  public void apply(DeltaSession session) {
    session.localUpdateAttribute(this.attributeName, this.attributeValue);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.attributeName = DataSerializer.readString(in);
    this.attributeValue = DataSerializer.readObject(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.attributeName, out);
    DataSerializer.writeObject(this.attributeValue, out);
  }
  
  public static void registerInstantiator(int id) {
    Instantiator.register(new Instantiator(DeltaSessionUpdateAttributeEvent.class, id) {
      public DataSerializable newInstance() {
        return new DeltaSessionUpdateAttributeEvent();
      }
    });
  }
  
  public String toString() {
    return new StringBuilder()
      .append("DeltaSessionUpdateAttributeEvent[")
      .append("attributeName=")
      .append(this.attributeName)
      .append("; attributeValue=")
      .append(this.attributeValue)
      .append("]")
      .toString();
  }
}

