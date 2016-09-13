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

package org.apache.geode.internal.cache;

import org.apache.geode.cache.*;
import org.apache.geode.internal.i18n.LocalizedStrings;

/**
 * TXEntryUserAttrState is the entity that tracks transactional changes
 * to an entry user attribute.
 *
 * 
 * @since GemFire 4.0
 * 
 */
public class TXEntryUserAttrState {
  private final Object originalValue;
  private Object pendingValue;

  public TXEntryUserAttrState(Object originalValue) 
  {
    this.originalValue = originalValue;
    this.pendingValue = originalValue;
  }
  public Object getOriginalValue() {
    return this.originalValue;
  }
  public Object getPendingValue() {
    return this.pendingValue;
  }
  public Object setPendingValue(Object pv) {
    Object result = this.pendingValue;
    this.pendingValue = pv;
    return result;
  }
  void checkForConflict(LocalRegion r, Object key) throws CommitConflictException {
    Object curCmtValue = r.basicGetEntryUserAttribute(key);
    if (this.originalValue != curCmtValue) {
      throw new CommitConflictException(LocalizedStrings.TXEntryUserAttrState_ENTRY_USER_ATTRIBUTE_FOR_KEY_0_ON_REGION_1_HAD_ALREADY_BEEN_CHANGED_TO_2.toLocalizedString(new Object[] {key, r.getFullPath(), curCmtValue}));
    }
  }
  void applyChanges(LocalRegion r, Object key) {
    try {
      Region.Entry re = r.getEntry(key);
      re.setUserAttribute(this.pendingValue);
    } catch (CacheRuntimeException ignore) {
      // ignore any exceptions since we have already locked and
      // found no conflicts.
    }
  }
}
