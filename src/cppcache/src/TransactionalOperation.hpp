#pragma once

#ifndef GEODE_TRANSACTIONALOPERATION_H_
#define GEODE_TRANSACTIONALOPERATION_H_

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
 * TransactionalOperation.hpp
 *
 *  Created on: 10-May-2011
 *      Author: ankurs
 */


#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include <gfcpp/Cacheable.hpp>
#include <gfcpp/VectorT.hpp>

namespace apache {
namespace geode {
namespace client {

enum ServerRegionOperation {
  GF_CONTAINS_KEY,
  GF_CONTAINS_VALUE,
  GF_CONTAINS_VALUE_FOR_KEY,
  GF_DESTROY,  // includes REMOVE(k,v)
  GF_EXECUTE_FUNCTION,
  GF_GET,
  GF_GET_ENTRY,
  GF_GET_ALL,
  GF_INVALIDATE,
  GF_REMOVE,
  GF_KEY_SET,
  GF_CREATE,
  GF_PUT,  // includes PUT_IF_ABSENT
  GF_PUT_ALL
};

_GF_PTR_DEF_(TransactionalOperation, TransactionalOperationPtr);

class TransactionalOperation : public apache::geode::client::SharedBase {
 public:
  TransactionalOperation(ServerRegionOperation op, const char* regionName,
                         CacheableKeyPtr key, VectorOfCacheablePtr arguments);
  virtual ~TransactionalOperation();

  CacheablePtr replay(Cache* cache);

 private:
  ServerRegionOperation m_operation;
  const char* m_regionName;
  CacheableKeyPtr m_key;
  VectorOfCacheablePtr m_arguments;
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_TRANSACTIONALOPERATION_H_
