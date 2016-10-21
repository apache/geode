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

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.modules.session.catalina.DeltaSession;
import org.apache.geode.modules.session.catalina.DeltaSessionInterface;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


@SuppressWarnings("serial")
public class DeltaSessionDestroyAttributeEvent implements DeltaSessionAttributeEvent {

  private String attributeName;

  public DeltaSessionDestroyAttributeEvent() {}

  public DeltaSessionDestroyAttributeEvent(String attributeName) {
    this.attributeName = attributeName;
  }

  public String getAttributeName() {
    return this.attributeName;
  }

  public void apply(DeltaSessionInterface session) {
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
    return new StringBuilder().append("DeltaSessionDestroyAttributeEvent[").append("attributeName=")
        .append(this.attributeName).append("]").toString();
  }
}

