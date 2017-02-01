#pragma once

#ifndef GEODE_GFCPP_SELECTRESULTS_H_
#define GEODE_GFCPP_SELECTRESULTS_H_

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

/**
 * @file
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "ExceptionTypes.hpp"
#include "Serializable.hpp"
#include "CacheableBuiltins.hpp"
namespace apache {
namespace geode {
namespace client {

class SelectResultsIterator;

/**
 * @class SelectResults SelectResults.hpp
 *
 * A SelectResults is obtained by executing a Query on the server.
 * This can either be a ResultSet or a StructSet.
 */
class CPPCACHE_EXPORT SelectResults : public SharedBase {
 public:
  /**
   * Check whether the SelectResults is modifiable.
   *
   * @returns false always at this time.
   */
  virtual bool isModifiable() const = 0;

  /**
   * Get the size of the SelectResults.
   *
   * @returns the number of items in the SelectResults.
   */
  virtual int32_t size() const = 0;

  /**
   * Index operator to directly access an item in the SelectResults.
   *
   * @param index the index number of the required item.
   * @throws IllegalArgumentException if the index is out of bounds.
   * @returns A smart pointer to the item indexed.
   */
  virtual const SerializablePtr operator[](int32_t index) const = 0;

  /**
   * Get a SelectResultsIterator with which to iterate over the items in the
   * SelectResults.
   *
   * @returns The SelectResultsIterator with which to iterate.
   */
  virtual SelectResultsIterator getIterator() = 0;

  /** Interface of an iterator for <code>SelectResults</code>.*/
  typedef CacheableVector::Iterator Iterator;

  /** Get an iterator pointing to the start of vector. */
  virtual Iterator begin() const = 0;

  /** Get an iterator pointing to the end of vector. */
  virtual Iterator end() const = 0;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_SELECTRESULTS_H_
