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

package org.apache.geode.modules.session.internal.filter.attributes;

import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.modules.session.internal.filter.GemfireHttpSession;

/**
 * Interface for session attribute storage. In reality, this interface is responsible for anything,
 * in the session which needs to be propagated for caching - as such it also includes other
 * 'attributes' such as maxInactiveInterval and lastAccessedTime
 */
public interface SessionAttributes extends DataSerializable {

  /**
   * Set the session to which these attributes belong.
   *
   * @param session the session to set
   */
  void setSession(GemfireHttpSession session);

  /**
   * Set an attribute value.
   *
   * @param attr the name of the attribute to set
   * @param value the value for the attribute
   * @return the value object
   */
  Object putAttribute(String attr, Object value);

  /**
   * Retrieve an attribute's value.
   *
   * @param attr the name of the attribute
   * @return the object associated with the attribute or null if none exists.
   */
  Object getAttribute(String attr);

  /**
   * Remove the named attribute.
   *
   * @param attr the name of the attribute to remove
   * @return the value of the attribute removed or null if the named attribute did not exist.
   */
  Object removeAttribute(String attr);

  /**
   * Return a set of all attribute names.
   *
   * @return a set of all attribute names
   */
  Set<String> getAttributeNames();

  /**
   * Set the max inactive interval for replication to other systems
   *
   * @param interval the time interval in seconds
   */
  void setMaxInactiveInterval(int interval);

  /**
   * Retrieve the max inactive interval
   *
   * @return the max inactive interval in seconds
   */
  int getMaxIntactiveInterval();

  /**
   * Set the last accessed time for replication to other systems
   *
   * @param time the last accessed time in milliseconds
   */
  void setLastAccessedTime(long time);

  /**
   * Return the last accessed time in milliseconds
   *
   * @return the last accessed time
   */
  long getLastAccessedTime();

  /**
   * Explicitly flush the attributes to backing store.
   */
  void flush();

  /**
   * Return the last jvm which 'owned' these attributes
   *
   * @return the jvmId
   */
  String getJvmOwnerId();

  /**
   * Set the jvmId. This is set every time the attributes are flushed to the cache.
   *
   */
  void setJvmOwnerId(String jvmId);

  /**
   * Return the creation of time of this session in milliseconds
   */
  long getCreationTime();

  /**
   * Set the creation time for these attributes
   */
  void setCreationTime(long creationTime);
}
