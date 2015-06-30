/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author rishim
 *
 */
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

	/**
	 * @param item
	 */
	public void addItem(Item item) {
		this.items.add(item);
	}

	/**
	 * 
	 */
	public Order() {
		this.items = new ArrayList<Item>();
	}

	/**
	 * @param id
	 * @param items
	 */
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
