/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerHelper;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.IOException;

public class InterestEvent {

  private Object key;
  private Object value;
  private boolean isDeserialized = false;


  public InterestEvent(Object key, Object value, boolean isDeserialized) {

    this.key = key;
    this.value = value;
    this.isDeserialized = isDeserialized;
  }

  /** Returns the key.
   * @return the key
   */
  public Object getKey() {
    return key;
  }





  public Object getValue() {

    if(isDeserialized || value == null) {
      return value;
    }

    try {
      value = CacheServerHelper.deserialize((byte[])value);
    } catch(IOException ioe) {
      throw new RuntimeException(LocalizedStrings.InterestEvent_IOEXCEPTION_DESERIALIZING_VALUE.toLocalizedString(), ioe);
    } catch(ClassNotFoundException cnfe) {
      throw new RuntimeException(LocalizedStrings.InterestEvent_CLASSNOTFOUNDEXCEPTION_DESERIALIZING_VALUE.toLocalizedString(), cnfe);
    }
    isDeserialized = true;

    return value;
  }


}
