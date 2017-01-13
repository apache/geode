#ifndef __GEMFIRE_STRUCTSET_H__
#define __GEMFIRE_STRUCTSET_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

#include "CqResults.hpp"
#include "Struct.hpp"

#include "SelectResultsIterator.hpp"

/**
 * @file
 */

namespace gemfire {
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

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_STRUCTSET_H__
