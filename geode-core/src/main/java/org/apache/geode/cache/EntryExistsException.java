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

package org.apache.geode.cache;

/**
 * Thrown when attempting to create a <code>Region.Entry</code> that already exists in the
 * <code>Region</code>.
 *
 * @see org.apache.geode.cache.Region#create(Object, Object)
 * @see Region.Entry
 * @since GemFire 3.0
 */
public class EntryExistsException extends CacheException {

  private static final long serialVersionUID = 2925082493103537925L;

  private Object oldValue;

  /**
   * Constructs an instance of <code>EntryExistsException</code> with the specified detail message.
   *
   * @param msg the detail message
   * @since GemFire 6.5
   */
  public EntryExistsException(String msg, Object oldValue) {
    super(msg);
    this.oldValue = oldValue;
  }

  /**
   * Returns the old existing value that caused this exception.
   */
  public Object getOldValue() {
    return oldValue;
  }

  /**
   * Sets the old existing value that caused this exception.
   */
  public void setOldValue(Object oldValue) {
    this.oldValue = oldValue;
  }

  @Override
  public String toString() {
    return super.toString() + ", with oldValue: " + oldValue;
  }
}
