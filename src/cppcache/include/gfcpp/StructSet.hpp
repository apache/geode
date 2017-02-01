#pragma once

#ifndef GEODE_GFCPP_STRUCTSET_H_
#define GEODE_GFCPP_STRUCTSET_H_

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

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

#include "CqResults.hpp"
#include "Struct.hpp"

#include "SelectResultsIterator.hpp"

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {
/**
 * @class StructSet StructSet.hpp
 *
 * A StructSet may be obtained after executing a Query which is obtained from a
 * QueryService which in turn is obtained from a Cache.
 * It is the parent of a Struct which contains the field values.
 */
class CPPCACHE_EXPORT StructSet : public CqResults {
 public:
  /**
   * Check whether the StructSet is modifiable.
   *
   * @returns false always at this time.
   */
  virtual bool isModifiable() const = 0;

  /**
   * Get the size of the StructSet.
   *
   * @returns the number of items in the StructSet.
   */
  virtual int32_t size() const = 0;

  /**
   * Index operator to directly access an item in the StructSet.
   *
   * @param index the index number of the item to get.
   * @throws IllegalArgumentException if the index is out of bounds.
   * @returns A smart pointer to the item indexed.
   */
  virtual const SerializablePtr operator[](int32_t index) const = 0;

  /**
   * Get the index number of the specified field name in the StructSet.
   *
   * @param fieldname the field name for which the index is required.
   * @returns the index number of the specified field name.
   * @throws IllegalArgumentException if the field name is not found.
   */
  virtual int32_t getFieldIndex(const char* fieldname) = 0;

  /**
   * Get the field name of the StructSet from the specified index number.
   *
   * @param index the index number of the field name to get.
   * @returns the field name from the specified index number or NULL if not
   * found.
   */
  virtual const char* getFieldName(int32_t index) = 0;

  /**
   * Get a SelectResultsIterator with which to iterate over the items in the
   * StructSet.
   *
   * @returns The SelectResultsIterator with which to iterate.
   */
  virtual SelectResultsIterator getIterator() = 0;

  /**
   * Destructor
   */
  virtual ~StructSet(){};
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_STRUCTSET_H_
