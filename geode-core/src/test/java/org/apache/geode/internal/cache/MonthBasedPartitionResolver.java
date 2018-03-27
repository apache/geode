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
package org.apache.geode.internal.cache;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.internal.cache.xmlcache.Declarable2;

/**
 * Example implementation of a Partition Resolver which uses part of the value for custom
 * partitioning.
 *
 * <p>
 * Extracted from {@link PRCustomPartitioningDistributedTest}.
 */
class MonthBasedPartitionResolver implements PartitionResolver, Declarable2 {

  private Properties properties;

  public MonthBasedPartitionResolver() {
    // nothing
  }

  @Override
  public Serializable getRoutingObject(EntryOperation opDetails) {
    Serializable routingObj = (Serializable) opDetails.getKey();
    Calendar calendar = Calendar.getInstance();
    calendar.setTime((Date) routingObj);
    return new SerializableMonth(calendar.get(Calendar.MONTH));
  }

  @Override
  public void close() {
    // nothing
  }

  @Override
  public void init(Properties props) {
    properties = props;
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this || obj instanceof MonthBasedPartitionResolver;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public Properties getConfig() {
    return properties;
  }
}
