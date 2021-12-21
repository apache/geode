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
package org.apache.geode.internal.cache.execute.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.cache.execute.PRColocationDUnitTestHelper;

public class Shipment implements DataSerializable {
  private String shipmentName;

  public Shipment() {

  }

  public Shipment(String shipmentName) {
    this.shipmentName = shipmentName + PRColocationDUnitTestHelper.getDefaultAddOnString();
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    shipmentName = DataSerializer.readString(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(shipmentName, out);
  }

  public String toString() {
    return shipmentName;
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof Shipment) {
      Shipment other = (Shipment) obj;
      return other.shipmentName != null && other.shipmentName.equals(shipmentName);
    }
    return false;
  }

  @Override
  public int hashCode() {
    if (shipmentName == null) {
      return super.hashCode();
    }
    return shipmentName.hashCode();
  }

}
