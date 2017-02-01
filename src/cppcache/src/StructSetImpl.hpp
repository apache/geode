#pragma once

#ifndef GEODE_STRUCTSETIMPL_H_
#define GEODE_STRUCTSETIMPL_H_

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

#include <gfcpp/StructSet.hpp>
#include <gfcpp/Struct.hpp>
#include <gfcpp/CacheableBuiltins.hpp>

#include <gfcpp/SelectResultsIterator.hpp>

#include <string>
#include <map>

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

class CPPCACHE_EXPORT StructSetImpl : public StructSet {
 public:
  StructSetImpl(const CacheableVectorPtr& values,
                const std::vector<CacheableStringPtr>& fieldNames);

  bool isModifiable() const;

  int32_t size() const;

  const SerializablePtr operator[](int32_t index) const;

  int32_t getFieldIndex(const char* fieldname);

  const char* getFieldName(int32_t index);

  SelectResultsIterator getIterator();

  /** Get an iterator pointing to the start of vector. */
  virtual SelectResults::Iterator begin() const;

  /** Get an iterator pointing to the end of vector. */
  virtual SelectResults::Iterator end() const;

  virtual ~StructSetImpl();

 private:
  CacheableVectorPtr m_structVector;

  std::map<std::string, int32_t> m_fieldNameIndexMap;

  int32_t m_nextIndex;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_STRUCTSETIMPL_H_
