#ifndef INTERNALCACHETRANSACTIONMANAGER2PC_H_
#define INTERNALCACHETRANSACTIONMANAGER2PC_H_

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "CacheTransactionManager.hpp"

namespace gemfire {

/**
 * Extension of the gemfire::CacheTransactionManager that enables client
 * application
 * to use Gemfire transaction as part of the global XA transaction.
 *
 * The prepare method of this class corresponds to the prepare phases of the
 * 2 phase commit protocol driven by a global transaction manager.
 *
 * The implementation of the gemfire::CacheTransactionManager commit() and
 * rollback()
 * methods must be 2 phase commit process aware.
 *
 * Methods of this class are expected to be called by a custom XA Resource
 * Manager
 * that is wrapping and adapting Gemfire client to XA specification
 * requirements.
 *
 * @since 8.3
 *
 */
class CPPCACHE_EXPORT InternalCacheTransactionManager2PC
    : public virtual gemfire::CacheTransactionManager {
 public:
  /**
   * Performs prepare during 2 phase commit completion.
   * Locks of the entries modified in the current transaction on the server
   * side.
   *
   * Calls to subsequent commit() or rollback() methods overridden by this class
   * are
   * expected to succeed after prepare() has returned successfully.
   * Gemfire commits internal transaction irreversibly on commit() call.
   *
   */
  virtual void prepare() = 0;

 protected:
  InternalCacheTransactionManager2PC();
  virtual ~InternalCacheTransactionManager2PC();
};
}

#endif /* INTERNALCACHETRANSACTIONMANAGER2PC_H_ */
