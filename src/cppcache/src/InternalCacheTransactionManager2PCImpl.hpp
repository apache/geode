/*=========================================================================
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * CacheTransactionManager2PCImpl.h
 *
 *  Created on: 13-Nov-2011
 *      Author: sshcherbakov
 */

#ifndef INTERNALCACHETRANSACTIONMANAGER2PCIMPL_H_
#define INTERNALCACHETRANSACTIONMANAGER2PCIMPL_H_

#include <gfcpp/InternalCacheTransactionManager2PC.hpp>
#include "CacheTransactionManagerImpl.hpp"

namespace gemfire {

class InternalCacheTransactionManager2PCImpl
    : public gemfire::CacheTransactionManagerImpl,
      public gemfire::InternalCacheTransactionManager2PC {
 public:
  InternalCacheTransactionManager2PCImpl(Cache* cache);
  virtual ~InternalCacheTransactionManager2PCImpl();

  virtual void prepare();
  virtual void commit();
  virtual void rollback();

 private:
  void afterCompletion(int32_t status);

  InternalCacheTransactionManager2PCImpl& operator=(
      const InternalCacheTransactionManager2PCImpl& other);
  InternalCacheTransactionManager2PCImpl(
      const InternalCacheTransactionManager2PCImpl& other);
};
}

#endif /* INTERNALCACHETRANSACTIONMANAGER2PCIMPL_H_ */
