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
package org.apache.geode.management.model;

import java.io.Serializable;

public class Item implements Serializable {
  private static final long serialVersionUID = -1174387683312780907L;
  private Order order;
  private String itemId;
  private String itemDescription;

  /**
   * @return the order
   */
  public Order getOrder() {
    return order;
  }

  /**
   * @param order the order to set
   */
  public void setOrder(Order order) {
    this.order = order;
  }

  /**
   * @return the itemId
   */
  public String getItemId() {
    return itemId;
  }

  /**
   * @param itemId the itemId to set
   */
  public void setItemId(String itemId) {
    this.itemId = itemId;
  }

  /**
   * @return the itemDescription
   */
  public String getItemDescription() {
    return itemDescription;
  }

  /**
   * @param itemDescription the itemDescription to set
   */
  public void setItemDescription(String itemDescription) {
    this.itemDescription = itemDescription;
  }

  public Item() {}

  public Item(Order order, String itemId, String itemDescription) {
    super();
    this.order = order;
    this.itemId = itemId;
    this.itemDescription = itemDescription;
  }

  @Override
  public String toString() {
    return "Item [order=" + order + ", itemId=" + itemId + "]";
  }
}
