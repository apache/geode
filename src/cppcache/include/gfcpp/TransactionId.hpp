/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TransactionId.h
 *
 *  Created on: 04-Feb-2011
 *      Author: ankurs
 */

#ifndef TRANSACTIONID_H_
#define TRANSACTIONID_H_

#include "SharedBase.hpp"

namespace gemfire {

/** The TransactionId interface is a "marker" interface that
* represents a unique GemFire transaction.
* @see Cache#getCacheTransactionManager
* @see CacheTransactionManager#getTransactionId
*/
class CPPCACHE_EXPORT TransactionId : public gemfire::SharedBase {
 protected:
  TransactionId();
  virtual ~TransactionId();
};
}

#endif /* TRANSACTIONID_H_ */
