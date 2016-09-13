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

package org.apache.geode.distributed.internal;

import java.util.HashSet;

import org.apache.geode.internal.i18n.LocalizedStrings;

/**
 * Provides an implementation of the {@link DistributionConfig} interface
 * for a snapshot of running application's configuration. The snapshot can be taken,
 * given to others, modified, and then applied to a currently running application.
 * <p> Settors will fail if called on attributes that can not be modified
 * when a system is running. 
 * <p> Instances should be obtained by calling
 * {@link RuntimeDistributionConfigImpl#takeSnapshot}.
 * <p/>
 * Removed implementations of hashCode() and equals() that were throwing
 * UnsupportedOperationException. See bug #50939 if you need to override those.
 */
public final class DistributionConfigSnapshot extends DistributionConfigImpl {
  private static final long serialVersionUID = 7445728132965092798L;

  private HashSet modifiable;
  
  /**
   * Constructs an internal system config given an existing one.
   * @param dc an existing system configuration.
   */
  public DistributionConfigSnapshot(DistributionConfig dc) {
    super(dc);
    this.modifiable  = new HashSet(20);
    String[] attNames = dc.getAttributeNames();
    for (int i=0; i < attNames.length; i++) {
      if (dc.isAttributeModifiable(attNames[i])) {
        this.modifiable.add(attNames[i]);
      }
    }
  }

  @Override
  protected String _getUnmodifiableMsg(String attName) {
    return LocalizedStrings.DistributionConfigSnapshot_THE_0_CONFIGURATION_ATTRIBUTE_CAN_NOT_BE_MODIFIED_WHILE_THE_SYSTEM_IS_RUNNING.toLocalizedString(attName);
  }

  @Override
  public boolean isAttributeModifiable(String attName) {
    checkAttributeName(attName);
    return modifiable.contains(attName);
  }

  @Override
  protected boolean _modifiableDefault() {
    return true;
  }
}
