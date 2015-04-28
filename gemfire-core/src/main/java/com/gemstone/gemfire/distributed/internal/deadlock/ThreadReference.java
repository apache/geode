/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.io.Serializable;

/**
 * A marker interface for thread identifiers returned from
 * {@link DeadlockDetector#getThreadReference(String, Thread)}
 * @author dsmith
 */
public interface ThreadReference extends Serializable {

}
