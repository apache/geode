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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

/**
 * The Order class is an abstraction modeling a order.
 * <p/>
 *
 * @since GemFire 8.0
 */

public class Order implements PdxSerializable {

  private Long purchaseOrderNo;
  private Long customerId;
  private String description;
  private Date orderDate;
  private Date deliveryDate;
  private String contact;
  private String email;
  private String phone;
  private List<Item> items;
  private double totalPrice;

  public Order() {
    items = new ArrayList<>();
  }

  public Order(final Long orderNo) {
    purchaseOrderNo = orderNo;
  }

  public Order(final Long orderNo, final Long custId, final String desc, final Date odate,
      final Date ddate, final String contact, final String email, final String phone,
      final List<Item> items, final double tprice) {
    purchaseOrderNo = orderNo;
    customerId = custId;
    description = desc;
    orderDate = odate;
    deliveryDate = ddate;
    this.contact = contact;
    this.email = email;
    this.phone = phone;
    this.items = items;
    totalPrice = tprice;
  }

  public void addItem(final Item item) {
    if (item != null)
      items.add(item);
  }

  @SuppressWarnings("unused")
  public Long getPurchaseOrderNo() {
    return purchaseOrderNo;
  }

  @SuppressWarnings("unused")
  public void setPurchaseOrderNo(Long purchaseOrderNo) {
    this.purchaseOrderNo = purchaseOrderNo;
  }

  public Long getCustomerId() {
    return customerId;
  }

  public void setCustomerId(Long customerId) {
    this.customerId = customerId;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @SuppressWarnings("unused")
  public Date getDeliveryDate() {
    return deliveryDate;
  }

  @SuppressWarnings("unused")
  public void setDeliveryDate(Date date) {
    deliveryDate = date;
  }

  public String getContact() {
    return contact;
  }

  public void setContact(String contact) {
    this.contact = contact;
  }

  @SuppressWarnings("unused")
  public String getEmail() {
    return email;
  }

  @SuppressWarnings("unused")
  public void setEmail(String email) {
    this.email = email;
  }

  @SuppressWarnings("unused")
  public String getPhone() {
    return phone;
  }

  @SuppressWarnings("unused")
  public void setPhone(String phone) {
    this.phone = phone;
  }

  public List<Item> getItems() {
    return items;
  }

  public void setItems(List<Item> items) {
    if (this.items == null)
      this.items = new ArrayList<>();

    this.items.addAll(items);
  }

  @SuppressWarnings("unused")
  public Date getOrderDate() {
    return orderDate;
  }

  @SuppressWarnings("unused")
  public void setOrderDate(Date orderDate) {
    this.orderDate = orderDate;
  }

  public double getTotalPrice() {
    return totalPrice;
  }

  public void setTotalPrice(double totalPrice) {
    this.totalPrice = totalPrice;
  }

  @Override
  public void toData(PdxWriter writer) {
    writer.writeLong("purchaseOrderNo", purchaseOrderNo);
    writer.writeLong("customerId", customerId);
    writer.writeString("description", description);
    writer.writeDate("orderDate", orderDate);
    writer.writeDate("deliveryDate", deliveryDate);
    writer.writeString("contact", contact);
    writer.writeString("email", email);
    writer.writeString("phone", phone);
    writer.writeObject("items", items);
    writer.writeDouble("totalPrice", totalPrice);
  }

  @Override
  public void fromData(PdxReader reader) {
    purchaseOrderNo = reader.readLong("purchaseOrderNo");
    customerId = reader.readLong("customerId");
    description = reader.readString("description");
    orderDate = reader.readDate("orderDate");
    deliveryDate = reader.readDate("deliveryDate");
    contact = reader.readString("contact");
    email = reader.readString("email");
    phone = reader.readString("phone");
    @SuppressWarnings("unchecked")
    final List<Item> items = (List<Item>) reader.readObject("items");
    this.items = items;
    totalPrice = reader.readDouble("totalPrice");

  }

}
