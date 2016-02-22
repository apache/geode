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
package com.gemstone.gemfire.management.internal.configuration.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberPattern;
import com.gemstone.gemfire.management.internal.configuration.domain.SharedConfigurationStatus;

/*****
 * 
 *
 */
public class SharedConfigurationStatusResponse implements DataSerializable {
  
  
  private SharedConfigurationStatus status;
  private static final long serialVersionUID = 1L;
  
  private Set<PersistentMemberPattern> waitingLocatorsInfo;
  
  public SharedConfigurationStatusResponse() {
  }

  public void setStatus(SharedConfigurationStatus status) {
    this.status = status;
  }
  
  public SharedConfigurationStatus getStatus() {
    return this.status;
  }
  
  public void addWaitingLocatorInfo(Set<PersistentMemberPattern> waitingLocatorsInfo) {
    this.waitingLocatorsInfo = waitingLocatorsInfo;
  }
  
  public Set<PersistentMemberPattern> getOtherLocatorInformation() {
    return this.waitingLocatorsInfo;
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeEnum(status, out);
    DataSerializer.writeHashSet((HashSet<?>) waitingLocatorsInfo, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.status = DataSerializer.readEnum(SharedConfigurationStatus.class, in);
    this.waitingLocatorsInfo = DataSerializer.readHashSet(in);
  }
  
  
}
