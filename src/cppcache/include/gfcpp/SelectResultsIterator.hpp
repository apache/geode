#ifndef __GEMFIRE_SELECTRESULTSITERATOR_H__
#define __GEMFIRE_SELECTRESULTSITERATOR_H__
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

#include "SelectResults.hpp"
#include "CacheableBuiltins.hpp"

namespace gemfire {

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

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_SELECTRESULTSITERATOR_H__
