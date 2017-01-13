#ifndef __GEMFIRE_STRUCTSETIMPL_H__
#define __GEMFIRE_STRUCTSETIMPL_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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

namespace gemfire {

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

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_STRUCTSETIMPL_H__
