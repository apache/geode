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
package org.apache.geode.rest.internal.web.controllers;


import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

/**
 * The Item class models item entity in the real world.
 * <p/>
 *
 * @since GemFire 8.0
 */


public class Item implements PdxSerializable {

  private Long itemNo;
  private String description;
  private int quantity;
  private float unitPrice;
  private float totalPrice;

  public Long getItemNo() {
    return itemNo;
  }

  public void setItemNo(Long itemNo) {
    this.itemNo = itemNo;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public int getQuantity() {
    return quantity;
  }

  public void setQuantity(int quantity) {
    this.quantity = quantity;
  }

  public float getUnitPrice() {
    return unitPrice;
  }

  public void setUnitPrice(final float unitprice) {
    this.unitPrice = unitprice;
  }

  public float getTotalPrice() {
    return totalPrice;
  }

  public void setTotalPrice(final float totalprice) {
    this.totalPrice = totalprice;
  }

  public Item() {

  }

  public Item(final Long itemNumber) {
    this.itemNo = itemNumber;
  }

  public Item(final Long itemNumber, final String desc, final int qty, final float uprice,
      final float tprice) {
    this.itemNo = itemNumber;
    this.description = desc;
    this.quantity = qty;
    this.unitPrice = uprice;
    this.totalPrice = tprice;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }

    if (!(obj instanceof Item)) {
      return false;
    }

    final Item that = (Item) obj;

    return (ObjectUtils.equals(this.getItemNo(), that.getItemNo())
        && ObjectUtils.equals(this.getDescription(), that.getDescription())
        && ObjectUtils.equals(this.getQuantity(), that.getQuantity())
        && ObjectUtils.equals(this.getQuantity(), that.getUnitPrice())
        && ObjectUtils.equals(this.getQuantity(), that.getTotalPrice()));
  }

  @Override
  public int hashCode() {
    int hashValue = 17;
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getItemNo());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getDescription());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getQuantity());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getUnitPrice());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getTotalPrice());
    return hashValue;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder("{ type = ");
    buffer.append(getClass().getName());
    buffer.append(", itemNo = ").append(getItemNo());
    buffer.append(", description = ").append(getDescription());
    buffer.append(", quantity = ").append(getQuantity());
    buffer.append(", unitPrice = ").append(getUnitPrice());
    buffer.append(", totalPrice = ").append(getTotalPrice());
    buffer.append(" }");
    return buffer.toString();
  }

  @Override
  public void toData(PdxWriter writer) {
    writer.writeLong("itemNo", itemNo);
    writer.writeString("description", description);
    writer.writeInt("quantity", quantity);
    writer.writeFloat("unitPrice", unitPrice);
    writer.writeFloat("totalPrice", totalPrice);
  }

  @Override
  public void fromData(PdxReader reader) {
    itemNo = reader.readLong("itemNo");
    description = reader.readString("description");
    quantity = reader.readInt("quantity");
    unitPrice = reader.readFloat("unitPrice");
    totalPrice = reader.readFloat("totalPrice");

  }

}
