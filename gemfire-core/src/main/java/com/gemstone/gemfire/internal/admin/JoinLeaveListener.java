/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
package com.gemstone.gemfire.internal.admin;

/**
 * Interface implemented by those who want to be alerted when a node joins or leaves a distributed GemFire system
 */
public interface JoinLeaveListener extends java.util.EventListener {
    public void nodeJoined( GfManagerAgent source, GemFireVM joined );
    public void nodeLeft( GfManagerAgent source, GemFireVM left );
    public void nodeCrashed( GfManagerAgent source, GemFireVM crashed );
}
