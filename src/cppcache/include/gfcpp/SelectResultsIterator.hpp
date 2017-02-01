#pragma once

#ifndef GEODE_GFCPP_SELECTRESULTSITERATOR_H_
#define GEODE_GFCPP_SELECTRESULTSITERATOR_H_

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

#include "SelectResults.hpp"
#include "CacheableBuiltins.hpp"

namespace apache {
namespace geode {
namespace client {

class ResultSetImpl;
class StructSetImpl;

/**
 * @class SelectResultsIterator SelectResultsIterator.hpp
 * A SelectResultsIterator is obtained from a ResultSet or StructSet and
 * is used to iterate over the items available in them.
 */
class CPPCACHE_EXPORT SelectResultsIterator : public SharedBase {
 public:
  /**
   * Check whether the SelectResultsIterator has another item to get.
   *
   * @returns true if another item is available otherwise false.
   */
  bool hasNext() const;

  /**
   * Get the next item from the SelectResultsIterator.
   *
   * @returns a smart pointer to the next item from the iterator or NULLPTR if
   * no further items are available.
   */
  const SerializablePtr next();

  /**
   * Move the iterator to point to the next item to get.
   *
   * @returns true if another item was available to move to otherwise false.
   */
  bool moveNext();

  /**
   * Get the current item pointed to by the SelectResultsIterator.
   *
   * @returns A smart pointer to the current item pointed to by the
   * SelectResultsIterator.
   */
  const SerializablePtr current() const;

  /**
   * Reset the SelectResultsIterator to point to the start of the items.
   */
  void reset();

 private:
  /**
   * Constructor - meant only for internal use.
   */
  SelectResultsIterator(const CacheableVectorPtr& vectorSR,
                        SelectResultsPtr srp);

  CacheableVectorPtr m_vectorSR;
  int32_t m_nextIndex;
  // this is to ensure that a reference of SelectResults is kept alive
  // if an iterator object is present
  SelectResultsPtr m_srp;

  friend class ResultSetImpl;
  friend class StructSetImpl;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_SELECTRESULTSITERATOR_H_
