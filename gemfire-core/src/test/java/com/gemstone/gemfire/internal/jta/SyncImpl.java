/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.jta;

import javax.transaction.Synchronization;

/*
 * @author Mitul Bid
 */
public class SyncImpl implements Synchronization {
	public boolean befCompletion = false ;
	public boolean aftCompletion = false ;
	
	public SyncImpl(){
	}
	
	public void beforeCompletion(){
		befCompletion = true;
	}
	
	public void afterCompletion(int status){
		aftCompletion = true;
	}
}
