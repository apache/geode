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
package com.gemstone.gemfire.modules.gatewaydelta;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@SuppressWarnings("serial")
public abstract class AbstractGatewayDeltaEvent implements GatewayDeltaEvent, DataSerializable {

  protected String regionName;
  protected String key;

  public AbstractGatewayDeltaEvent() {
  }

  public AbstractGatewayDeltaEvent(String regionName, String key) {
    this.regionName = regionName;
    this.key = key;
  }

  public String getRegionName() {
    return this.regionName;
  }

  public String getKey() {
    return this.key;
  }

  @SuppressWarnings("unchecked")
  public Region getRegion(Cache cache) {
    return cache.getRegion(this.regionName);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.regionName = DataSerializer.readString(in);
    this.key = DataSerializer.readString(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.regionName, out);
    DataSerializer.writeString(this.key, out);
  }
}
