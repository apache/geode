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

public class PhoneNo implements Serializable {
  public int phoneNo1;
  public int phoneNo2;
  public int phoneNo3;
  public int mobile;

  /** Creates a new instance of PhoneNo */
  public PhoneNo(int i, int j, int k, int m) {
    this.phoneNo1 = i;
    this.phoneNo2 = j;
    this.phoneNo3 = k;
    this.mobile = m;
  }

}// end of class
