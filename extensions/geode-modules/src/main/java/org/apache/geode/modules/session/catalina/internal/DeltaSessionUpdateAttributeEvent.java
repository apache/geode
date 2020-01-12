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
package org.apache.geode.modules.session.catalina.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.modules.session.catalina.DeltaSessionInterface;

@SuppressWarnings("serial")
public class DeltaSessionUpdateAttributeEvent implements DeltaSessionAttributeEvent {
  private String attributeName;
  private Object attributeValue;

  String getAttributeName() {
    return attributeName;
  }

  Object getAttributeValue() {
    return attributeValue;
  }

  @SuppressWarnings("unused")
  public DeltaSessionUpdateAttributeEvent() {}

  public DeltaSessionUpdateAttributeEvent(String attributeName, Object attributeValue) {
    this.attributeName = attributeName;
    this.attributeValue = attributeValue;
  }

  @Override
  public void apply(DeltaSessionInterface session) {
    session.localUpdateAttribute(this.attributeName, this.attributeValue);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.attributeName = DataSerializer.readString(in);
    this.attributeValue = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.attributeName, out);
    DataSerializer.writeObject(this.attributeValue, out);
  }

  public String toString() {
    return "DeltaSessionUpdateAttributeEvent[" + "attributeName="
        + this.attributeName + "; attributeValue=" + this.attributeValue
        + "]";
  }
}
