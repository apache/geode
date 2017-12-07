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
package org.apache.geode.experimental.driver;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;

/**
 * Encapsulates the attributes of a region in a GemFire server.
 *
 * <strong>This code is an experimental prototype and is presented "as is" with no warranty,
 * suitability, or fitness of purpose implied.</strong>
 */
@Experimental
public class RegionAttributes {
  /**
   * String that uniquely identifies the region within a GemFire server.
   */
  public String name;

  /**
   * Specifies how a local cache will handle the data for the region.
   */
  public String dataPolicy;

  /**
   * Whether modifications to the region are acknowledged throughout the GemFire distributed system.
   */
  public String scope;

  /**
   * String that is the fully-qualified class name of the type of which all keys must be instances.
   * May be <code>null</code>.
   */
  public String keyConstraint;

  /**
   * String that is the fully-qualified class name of the type of which all values must be
   * instances. May be <code>null</code>.
   */
  public String valueConstraint;

  /**
   * Whether the region is persisted to disk.
   */
  public boolean persisted;

  /**
   * Number of key/value pairs in the region.
   */
  public long size;

  /**
   * Creates an encapsulation of region attributes.
   *
   * @param region Protobuf encoded region attributes.
   */
  public RegionAttributes(BasicTypes.Region region) {
    this.name = region.getName();
    this.dataPolicy = region.getDataPolicy();
    this.scope = region.getScope();
    this.keyConstraint = region.getKeyConstraint();
    this.valueConstraint = region.getValueConstraint();
    this.persisted = region.getPersisted();
    this.size = region.getSize();
  }
}
