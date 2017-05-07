/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PdxSerializable.cpp
 *
 *  Created on: Sep 29, 2011
 *      Author: npatel
 */

#include "PdxSerializable.hpp"
#include "impl/GemfireTypeIdsImpl.hpp"
#include "CacheableString.hpp"
#include "impl/PdxHelper.hpp"
#include "CacheableKeys.hpp"

namespace gemfire
{
  PdxSerializable::PdxSerializable() {

  }

  PdxSerializable::~PdxSerializable() {

  }

  int8_t PdxSerializable::typeId( ) const
  {
	return (int8_t) GemfireTypeIdsImpl::PDX;
  }

  void PdxSerializable::toData(DataOutput& output) const
  {
	LOGDEBUG("SerRegistry.cpp:serializePdx:86: PDX Object Type = %s", typeid(*this).name());
	PdxHelper::serializePdx(output, *this);
  }

  Serializable* PdxSerializable::fromData( DataInput& input )
  {
  	throw UnsupportedOperationException("operation PdxSerializable::fromData() is not supported ");
    /* adongre  - Coverity II
     * CID 29295: Structurally dead code (UNREACHABLE) 
     * This code cannot be reached: "return this;". 
     * Fix : Remove this line 
     */
     //return this;
  }

  CacheableStringPtr PdxSerializable::toString() const {
    return CacheableString::create( "PdxSerializable" );
  }

  bool PdxSerializable::operator==( const CacheableKey& other ) const{
    return ( this == &other );
  }


  uint32_t PdxSerializable::hashcode( ) const{
    uint64_t hash = (uint64_t)(intptr_t)this;
    return gemfire::serializer::hashcode(hash);
  }
}

