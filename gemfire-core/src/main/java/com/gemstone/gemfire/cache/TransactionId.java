/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

import java.io.Externalizable;

/** The TransactionId interface is a "marker" interface that
 * represents a unique GemFire transaction.
 *
 * @author Mitch Thomas
 * 
 * @since 4.0
 * 
 * @see Cache#getCacheTransactionManager
 * @see CacheTransactionManager#getTransactionId
 */
public interface TransactionId extends Externalizable {
}
