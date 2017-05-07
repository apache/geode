#ifndef __GEMFIRE_RESULTSETIMPL_H__
#define __GEMFIRE_RESULTSETIMPL_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "../gfcpp_globals.hpp"
#include "../gf_types.hpp"
#include "../ExceptionTypes.hpp"

#include "../ResultSet.hpp"
#include "../CacheableBuiltins.hpp"
#include "../SelectResultsIterator.hpp"

/**
 * @file
 */

namespace gemfire
{

class CPPCACHE_EXPORT ResultSetImpl : public ResultSet
{

public:

  ResultSetImpl(const CacheableVectorPtr& response);

  bool isModifiable() const;

  int32_t size() const;

  const SerializablePtr operator [] (int32_t index) const;

  SelectResultsIterator getIterator();

  /** Get an iterator pointing to the start of vector. */
  virtual SelectResults::Iterator begin( ) const;

  /** Get an iterator pointing to the end of vector. */
  virtual SelectResults::Iterator end( ) const;

  ~ResultSetImpl();

private:

  CacheableVectorPtr m_resultSetVector;
  int32_t m_nextIndex;
};

} //namespace gemfire

#endif //ifndef __GEMFIRE_RESULTSETIMPL_H__
