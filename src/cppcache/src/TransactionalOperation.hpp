/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TransactionalOperation.hpp
 *
 *  Created on: 10-May-2011
 *      Author: ankurs
 */

#ifndef TRANSACTIONALOPERATION_HPP_
#define TRANSACTIONALOPERATION_HPP_

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include <gfcpp/Cacheable.hpp>
#include <gfcpp/VectorT.hpp>

namespace gemfire {

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

class TransactionalOperation : public gemfire::SharedBase {
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
}

#endif /* TRANSACTIONALOPERATION_HPP_ */
