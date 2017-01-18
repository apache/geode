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
}  // namespace gemfire

#endif /* INTERNALCACHETRANSACTIONMANAGER2PCIMPL_H_ */
