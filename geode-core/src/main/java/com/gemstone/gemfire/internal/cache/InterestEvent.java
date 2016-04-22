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
