/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.rest.internal.web.controllers;

import java.io.Serializable;

import com.gemstone.gemfire.internal.lang.ObjectUtils;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;

/**
 * The Item class models item entity in the real world.
 * <p/>
 * @author Nilkanth Patel
 * @since 8.0
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

  public Item(final Long itemNumber, final String desc, final int qty, final float uprice, final float tprice) {
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
