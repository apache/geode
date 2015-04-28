/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * TXEntryUserAttrState is the entity that tracks transactional changes
 * to an entry user attribute.
 *
 * @author Darrel Schneider
 * 
 * @since 4.0
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
