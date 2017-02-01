#pragma once

#ifndef GEODE_RESULTSETIMPL_H_
#define GEODE_RESULTSETIMPL_H_

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

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include <gfcpp/ExceptionTypes.hpp>

#include <gfcpp/ResultSet.hpp>
#include <gfcpp/CacheableBuiltins.hpp>
#include <gfcpp/SelectResultsIterator.hpp>

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

class CPPCACHE_EXPORT ResultSetImpl : public ResultSet {
 public:
  ResultSetImpl(const CacheableVectorPtr& response);

  bool isModifiable() const;

  int32_t size() const;

  const SerializablePtr operator[](int32_t index) const;

  SelectResultsIterator getIterator();

  /** Get an iterator pointing to the start of vector. */
  virtual SelectResults::Iterator begin() const;

  /** Get an iterator pointing to the end of vector. */
  virtual SelectResults::Iterator end() const;

  ~ResultSetImpl();

 private:
  CacheableVectorPtr m_resultSetVector;
  // UNUSED int32_t m_nextIndex;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_RESULTSETIMPL_H_
