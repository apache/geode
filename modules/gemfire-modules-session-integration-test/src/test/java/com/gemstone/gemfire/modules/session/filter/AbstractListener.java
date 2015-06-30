/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.gemstone.gemfire.modules.session.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author jdeppe
 */
public abstract class AbstractListener {

  protected final List<ListenerEventType> events =
      new ArrayList<ListenerEventType>();

  protected CountDownLatch latch;

  public AbstractListener() {
    this(1);
  }

  public AbstractListener(int numCalls) {
    latch = new CountDownLatch(numCalls);
  }

  public void setLatch(int numCalls) {
    latch = new CountDownLatch(numCalls);
  }

  public boolean await(long timeout,
      TimeUnit unit) throws InterruptedException {
    return latch.await(timeout, unit);
  }

  public List<ListenerEventType> getEvents() {
    return events;
  }
}
