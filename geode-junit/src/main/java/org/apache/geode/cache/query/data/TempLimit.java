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
package org.apache.geode.cache.query.data;

import java.io.Serializable;

public class TempLimit implements Serializable {
  long id;
  long originalId; // linked back to Limit->id
  Limit limit;

  public long getOriginalId() {
    return originalId;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public Limit getLimit() {
    return limit;
  }

  public TempLimit setOriginalId(long originalId) {
    this.originalId = originalId;
    return this;
  }

  public TempLimit setLimit(Limit limit) {
    this.limit = limit;
    return this;
  }

  public TempLimit(long id, long originalId, Limit limit) {
    this.id = id;
    this.originalId = originalId;
    this.limit = limit;
  }

  @Override
  public String toString() {
    return "Limit{" +
        "id=" + id +
        '}';
  }
}
