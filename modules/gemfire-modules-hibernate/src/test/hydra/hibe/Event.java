/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package hibe;

import java.util.Date;

public class Event {
    private Long id;

    private String title;
    private Date date;
    private int i;

    public Event() {}

    public Long getId() {
        return id;
    }

    private void setId(Long id) {
        this.id = id;
    }

    public Date getDate() {
        return date;
    }

    public Integer getVersion() {
    	return i;
    }
    
    public void setVersion(int i) {
    	this.i = i;
    }
    
    public void setDate(Date date) {
        this.date = date;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
    @Override
    public String toString() {
    	StringBuilder b = new StringBuilder();
    	b.append("Event:id:"+id+" title:"+title+" date:"+date);
    	return b.toString();
    }
}
