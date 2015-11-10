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
package com.gemstone.gemfire.management.internal.cli.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/****
 * Data class used for sending back names for the durable client cq
 * for a client
 */
public class DurableCqNamesResult extends MemberResult implements Serializable{

  private static final long serialVersionUID = 1L;
  private List<String> cqNames = new ArrayList<String>();
  
  public DurableCqNamesResult(final String memberNameOrId) {
	 super(memberNameOrId);
  }
  public DurableCqNamesResult (final String memberNameOrId, List<String> cqNames) {
	  super(memberNameOrId);
	  cqNames.addAll(cqNames);
  }
  
  public void addCqName(String cqName) {
    cqNames.add(cqName);
    super.isSuccessful = true;
    super.opPossible = true;
  }
  
  public List<String> getCqNamesList() {
    return cqNames;
  }
  
  public void setCqNamesList(List<String> cqNamesList) {
	  this.cqNames.addAll(cqNamesList);
	  this.isSuccessful = true;
	  this.opPossible = true;
  }
  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(super.toString());
    for (String cqName: cqNames) {
      sb.append("\nCqName : " + cqName);
    }
    return sb.toString();
  }
}
