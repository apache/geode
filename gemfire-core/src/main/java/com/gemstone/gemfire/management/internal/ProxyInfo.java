/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

import javax.management.ObjectName;

/**
 * Proxy info class holds details about proxy
 * for quicker access during several Management 
 * operations
 * @author rishim
 *
 */
public class ProxyInfo {
	
	/**
	 * Proxy Interface
	 */
  private Class proxyInterface;
	/**
	 * proxy instance
	 */
	private Object proxyInstance;

	/**
	 * JMX name of proxy
	 */
	private ObjectName objectName;
	

	/**
	 * public constructor
	 * @param proxyInstance
	 * @param objectName
	 */
	public ProxyInfo(Class proxyInterface, Object proxyInstance,  ObjectName objectName){
		this.proxyInstance = proxyInstance;
		this.proxyInterface = proxyInterface;
		this.objectName = objectName;
		
	}
	
	/**
	 * get the proxy instance
	 * @return proxyInstance
	 */
	public Object getProxyInstance() {
		return proxyInstance;
	}


	/**
	 * get MBean name
	 * @return ObjectName
	 */
	public ObjectName getObjectName() {
		return objectName;
	}

  public Class getProxyInterface() {
    return proxyInterface;
  }

  public void setProxyInterface(Class proxyInterface) {
    this.proxyInterface = proxyInterface;
  }


}
