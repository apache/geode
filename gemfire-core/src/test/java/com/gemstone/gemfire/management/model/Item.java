/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.model;

import java.io.Serializable;

/**
 * @author rishim
 *
 */
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

	/**
	 * 
	 */
	public Item() {
	}

	/**
	 * @param order
	 * @param itemId
	 * @param itemDescription
	 */
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
