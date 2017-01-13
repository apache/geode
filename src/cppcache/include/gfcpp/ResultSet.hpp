#ifndef __GEMFIRE_RESULTSET_H__
#define __GEMFIRE_RESULTSET_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "ExceptionTypes.hpp"

#include "SelectResults.hpp"
#include "VectorT.hpp"

#include "SelectResultsIterator.hpp"

/**
 * @file
 */

namespace gemfire {

/**
 * @class ResultSet ResultSet.hpp
 * A ResultSet may be obtained after executing a Query which is obtained from a
 * QueryService which in turn is obtained from a Cache.
 */
class CPPCACHE_EXPORT ResultSet : public SelectResults {
 public:
  /**
   * Check whether the ResultSet is modifiable.
   *
   * @returns false always at this time.
   */
  virtual bool isModifiable() const = 0;

  /**
   * Get the size of the ResultSet.
   *
   * @returns the number of items in the ResultSet.
   */
  virtual int32_t size() const = 0;

  /**
   * Index operator to directly access an item in the ResultSet.
   *
   * @param index the index number of the required item.
   * @throws IllegalArgumentException if the index is out of bounds.
   * @returns A smart pointer to the item indexed.
   */
  virtual const SerializablePtr operator[](int32_t index) const = 0;

  /**
   * Get a SelectResultsIterator with which to iterate over the items in the
   * ResultSet.
   *
   * @returns The SelectResultsIterator with which to iterate.
   */
  virtual SelectResultsIterator getIterator() = 0;

  /**
   * Destructor
   */
  virtual ~ResultSet(){};
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_RESULTSET_H__
