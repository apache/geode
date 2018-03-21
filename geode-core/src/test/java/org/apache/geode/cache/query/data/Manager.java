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

import java.util.Set;

public class Manager extends Employee {

  public int manager_id;

  /** Creates a new instance of Manager */
  public Manager(String name, int age, int empId, String title, int salary, Set addresses,
      int mgrId) {
    super(name, age, empId, title, salary, addresses);
    this.manager_id = mgrId;
  }

  public int getManager_id() {
    return this.manager_id;
  }
}
