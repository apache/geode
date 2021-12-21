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
/*
 * Created on Apr 18, 2005
 *
 *
 */
package org.apache.geode.internal.cache.xmlcache;

import java.util.List;
import java.util.Map;

/**
 * This class represents the data given for binding a DataSource to JNDI tree. It encapsulates to
 * Map objects , one for gemfire jndi tree specific data & another for vendor specific data. This
 * object will get created for every <jndi-binding></jndi-binding>
 *
 */
public class BindingCreation {

  private Map gfSpecific = null;
  // private Map vendorSpecific = null;
  private List vendorSpecificList = null;

  public BindingCreation(Map gfSpecific, List vendorSpecific) {
    this.gfSpecific = gfSpecific;
    vendorSpecificList = vendorSpecific;
  }

  /**
   * This function returns the VendorSpecific data Map
   *
   */
  List getVendorSpecificList() {
    return vendorSpecificList;
  }

  /**
   * This function returns the Gemfire Specific data Map
   *
   */
  Map getGFSpecificMap() {
    return gfSpecific;
  }
}
