#ifndef __GEMFIRE_SELECTRESULTS_H__
#define __GEMFIRE_SELECTRESULTS_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "ExceptionTypes.hpp"
#include "Serializable.hpp"
#include "CacheableBuiltins.hpp"
namespace gemfire {

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

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_SELECTRESULTS_H__
