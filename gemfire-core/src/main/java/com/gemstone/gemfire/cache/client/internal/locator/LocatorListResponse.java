/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import com.gemstone.gemfire.internal.DataSerializableFixedID;

/**
 * @author dsmith
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
