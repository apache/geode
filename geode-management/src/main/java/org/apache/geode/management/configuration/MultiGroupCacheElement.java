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

package org.apache.geode.management.configuration;

import java.util.List;

import javax.xml.bind.annotation.XmlTransient;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.annotations.Experimental;

/**
 * Implement this interface only when the CacheElement can span multiple groups.
 *
 * The implementation of getGroup should already be implemented by CacheElement
 *
 * If implementation of getGroups should be simply "return groups" if the object is already a
 * CacheElement.
 *
 * When implementing this interface, the CacheElement also needs to specifically implement
 * Object.equals() method
 */
@Experimental
public interface MultiGroupCacheElement {
  @XmlTransient
  List<String> getGroups();

  // this is needed to hide "group" attribute in json serialization
  @XmlTransient
  @JsonIgnore
  String getGroup();
}
