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
import java.util.ArrayList;
import java.util.Collection;

public class PortfolioWithMetrics implements Serializable {
  long id;
  Collection<PortfolioMetric> metrics = new ArrayList<>();

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public Collection<PortfolioMetric> getMetrics() {
    return metrics;
  }

  public void setMetrics(Collection<PortfolioMetric> metrics) {
    this.metrics = metrics;
  }

  public PortfolioWithMetrics(long l) {
    this.id = l;
  }

  public PortfolioWithMetrics(String p1, String s, String p11) {

  }

  @Override
  public String toString() {
    return "Portfolio{" +
        "id=" + id +
        ", metrics=" + metrics +
        '}';
  }
}
