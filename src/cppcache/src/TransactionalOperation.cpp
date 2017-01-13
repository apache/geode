/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TransactionalOperation.cpp
 *
 *  Created on: 10-May-2011
 *      Author: ankurs
 */

#include "TransactionalOperation.hpp"
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/Cache.hpp>
#include <gfcpp/FunctionService.hpp>

namespace gemfire {

TransactionalOperation::TransactionalOperation(ServerRegionOperation op,
                                               const char* regionName,
                                               CacheableKeyPtr key,
                                               VectorOfCacheablePtr arguments)
    : m_operation(op),
      m_regionName(regionName),
      m_key(key),
      m_arguments(arguments) {}

TransactionalOperation::~TransactionalOperation() {}

CacheablePtr TransactionalOperation::replay(Cache* cache) {
  CacheablePtr result = NULLPTR;

  switch (m_operation) {
    case GF_CONTAINS_KEY:
      result = CacheableBoolean::create(
          cache->getRegion(m_regionName)->containsKeyOnServer(m_key));
      break;
    case GF_CONTAINS_VALUE:
      GfErrTypeThrowException("Contains value not supported in transaction",
                              GF_NOTSUP);
      // result = Boolean.valueOf(containsValue(args[0], true));
      break;
    case GF_CONTAINS_VALUE_FOR_KEY:
      result = CacheableBoolean::create(
          cache->getRegion(m_regionName)->containsValueForKey(m_key));
      break;
    case GF_DESTROY:  // includes REMOVE(k,v)
      cache->getRegion(m_regionName)->destroy(m_key, m_arguments->at(0));
      break;
    case GF_EXECUTE_FUNCTION: {
      ExecutionPtr execution;
      if (m_regionName == NULL) {
        execution = FunctionService::onServer(CachePtr(cache));
      } else {
        execution = FunctionService::onRegion(cache->getRegion(m_regionName));
      }
      result =
          execution->withArgs(m_arguments->at(0))
              ->withFilter(m_arguments->at(1))
              ->withCollector(m_arguments->at(2))
              ->execute(m_arguments->at(3)->toString()->asChar(),
                        (static_cast<CacheableInt32Ptr>(m_arguments->at(4)))
                            ->value());
    } break;
    case GF_GET:
      result = cache->getRegion(m_regionName)->get(m_key, m_arguments->at(0));
      break;
    case GF_GET_ENTRY:
      GfErrTypeThrowException("getEntry not supported in transaction",
                              GF_NOTSUP);
      break;
    case GF_GET_ALL:
      cache->getRegion(m_regionName)
          ->getAll(
              *(static_cast<VectorOfCacheableKeyPtr>(m_arguments->at(0))).ptr(),
              m_arguments->at(1), m_arguments->at(2),
              (static_cast<CacheableBooleanPtr>(m_arguments->at(3)))->value());
      break;
    case GF_INVALIDATE:
      cache->getRegion(m_regionName)->invalidate(m_key, m_arguments->at(0));
      break;
    case GF_REMOVE:
      cache->getRegion(m_regionName)
          ->remove(m_key, m_arguments->at(0), m_arguments->at(1));
      break;
    case GF_KEY_SET:
      cache->getRegion(m_regionName)
          ->serverKeys(
              *(static_cast<VectorOfCacheableKeyPtr>(m_arguments->at(0)))
                   .ptr());
      break;
    case GF_CREATE:  // includes PUT_IF_ABSENT
      cache->getRegion(m_regionName)
          ->create(m_key, m_arguments->at(0), m_arguments->at(1));
      break;
    case GF_PUT:  // includes PUT_IF_ABSENT
      cache->getRegion(m_regionName)
          ->put(m_key, m_arguments->at(0), m_arguments->at(1));
      break;
    case GF_PUT_ALL:
      cache->getRegion(m_regionName)
          ->putAll(
              *(static_cast<HashMapOfCacheablePtr>(m_arguments->at(0))).ptr(),
              (static_cast<CacheableInt32Ptr>(m_arguments->at(1)))->value());
      break;
    default:
      throw UnsupportedOperationException(
          "unknown operation encountered during transaction replay");
  }

  return result;
}
}  // namespace gemfire
