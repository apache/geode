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
public class DeltaSessionDestroyAttributeEvent implements DeltaSessionAttributeEvent {

  private String attributeName;
  
  public DeltaSessionDestroyAttributeEvent() {
  }

  public DeltaSessionDestroyAttributeEvent(String attributeName) {
    this.attributeName = attributeName;
  }
  
  public String getAttributeName() {
    return this.attributeName;
  }

  public void apply(DeltaSession session) {
    session.localDestroyAttribute(this.attributeName);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.attributeName = DataSerializer.readString(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.attributeName, out);
  }
  
  public static void registerInstantiator(int id) {
    Instantiator.register(new Instantiator(DeltaSessionDestroyAttributeEvent.class, id) {
      public DataSerializable newInstance() {
        return new DeltaSessionDestroyAttributeEvent();
      }
    });
  }
  
  public String toString() {
    return new StringBuilder()
      .append("DeltaSessionDestroyAttributeEvent[")
      .append("attributeName=")
      .append(this.attributeName)
      .append("]")
      .toString();
  }
}

