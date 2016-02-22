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
package com.gemstone.gemfire.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import com.gemstone.gemfire.internal.DataSerializableFixedID;

/**
 *
 */
public class LocatorListResponse extends ServerLocationResponse {
  /** ArrayList of ServerLocations for controllers */
  private ArrayList controllers;
  private boolean isBalanced;
  private boolean locatorsFound = false;

  /** Used by DataSerializer */
  public LocatorListResponse() {
  }
  
  public LocatorListResponse(ArrayList locators, boolean isBalanced) {
    this.controllers = locators;
    if (locators != null && !locators.isEmpty()) {
      this.locatorsFound = true;
    }
    this.isBalanced = isBalanced;
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.controllers = SerializationHelper.readServerLocationList(in);
    this.isBalanced = in.readBoolean();
    if (this.controllers != null && !this.controllers.isEmpty()) {
      this.locatorsFound = true;
    }
  }

  public void toData(DataOutput out) throws IOException {
    SerializationHelper.writeServerLocationList(this.controllers, out);
    out.writeBoolean(isBalanced);
  }

  /**
   * Returns an array list of type ServerLocation containing controllers.
   * @return list of controllers
   */
  public ArrayList getLocators() {
    return this.controllers;
  }

  /**
   * Returns whether or not the locator thinks that the servers 
   * in this group are currently balanced.
   * @return true if the servers are balanced.
   */
  public boolean isBalanced() {
    return isBalanced;
  }
  
  @Override
  public String toString() {
    return "LocatorListResponse{locators=" + controllers + ",isBalanced=" + isBalanced + "}";
  }

  public int getDSFID() {
    return DataSerializableFixedID.LOCATOR_LIST_RESPONSE;
  }

  @Override
  public boolean hasResult() {
    return this.locatorsFound;
  }
  
}
