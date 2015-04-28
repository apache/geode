/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

/**
 * GemFire Resource Events
 * @author rishim
 *
 */
public enum ResourceEvent {
    CACHE_CREATE, 
    REGION_CREATE, 
    DISKSTORE_CREATE, 
    GATEWAYSENDER_CREATE, 
    LOCKSERVICE_CREATE, 
    CACHE_REMOVE, 
    REGION_REMOVE, 
    DISKSTORE_REMOVE, 
    GATEWAYSENDER_REMOVE, 
    LOCKSERVICE_REMOVE,
    MANAGER_CREATE,
    MANAGER_START,
    MANAGER_STOP,
    LOCATOR_START,
    ASYNCEVENTQUEUE_CREATE,
    SYSTEM_ALERT,
    CACHE_SERVER_START,
    CACHE_SERVER_STOP,
    GATEWAYRECEIVER_START,
    GATEWAYRECEIVER_STOP,
    GATEWAYRECEIVER_CREATE,
    GATEWAYSENDER_START, 
    GATEWAYSENDER_STOP, 
    GATEWAYSENDER_PAUSE, 
    GATEWAYSENDER_RESUME 
    
}

