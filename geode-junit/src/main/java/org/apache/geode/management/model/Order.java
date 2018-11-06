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
import java.util.ArrayList;
import java.util.Collection;

public class Order implements Serializable {
  private static final long serialVersionUID = 2049641616996906290L;
  private String id;
  private Collection<Item> items;

  /**
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * @return the items
   */
  public Collection<Item> getItems() {
    return items;
  }

  /**
   * @param items the items to set
   */
  public void setItems(Collection<Item> items) {
    this.items = items;
  }

  public void addItem(Item item) {
    this.items.add(item);
  }

  public Order() {
    this.items = new ArrayList<Item>();
  }

  public Order(String id, Collection<Item> items) {
    super();
    this.id = id;
    this.items = items;
  }

  @Override
  public String toString() {
    return "Order [id=" + id + "]";
  }
}
