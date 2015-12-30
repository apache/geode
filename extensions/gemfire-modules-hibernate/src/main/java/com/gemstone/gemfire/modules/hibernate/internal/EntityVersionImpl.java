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
package com.gemstone.gemfire.modules.hibernate.internal;

/**
 * 
 * @author sbawaska
 */
public class EntityVersionImpl implements EntityVersion {

  private final Long version;

  public EntityVersionImpl(Long version) {
    this.version = version;
  }

  @Override
  public Long getVersion() {
    return this.version;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof EntityVersionImpl) {
      EntityVersionImpl other = (EntityVersionImpl)obj;
      if (this.version.equals(other.version)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.version.hashCode();
  }
}
