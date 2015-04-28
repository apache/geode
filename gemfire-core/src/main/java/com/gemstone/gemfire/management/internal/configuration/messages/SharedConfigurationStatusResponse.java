/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author bansods
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
