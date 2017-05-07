/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "gfcpp_globals.hpp"
#include "Serializable.hpp"
#include "impl/GemfireTypeIdsImpl.hpp"
#include "impl/SerializationRegistry.hpp"
#include "impl/Utils.hpp"
#include "CacheableString.hpp"


namespace gemfire {

  int8_t Serializable::typeId( ) const
  {
    int32_t classIdToCheck = classId();
    if (classIdToCheck <= 127 && classIdToCheck >= -128) {
      return (int8_t) GemfireTypeIdsImpl::CacheableUserData;
    }
    else if (classIdToCheck <= 32767 && classIdToCheck >= -32768) {
      return (int8_t) GemfireTypeIdsImpl::CacheableUserData2;
    }
    else {
      return (int8_t) GemfireTypeIdsImpl::CacheableUserData4;
    }
  }

  int8_t Serializable::DSFID( ) const
  {
    return (int8_t) GemfireTypeIdsImpl::FixedIDDefault;
  }

  uint32_t Serializable::objectSize() const
  {
    return 0;
  }

  void Serializable::registerType( TypeFactoryMethod creationFunction )
  {
    SerializationRegistry::addType( creationFunction );
  }

  void Serializable::registerPdxType( TypeFactoryMethodPdx creationFunction )
  {
  	SerializationRegistry::addPdxType( creationFunction );
  }

  void Serializable::registerPdxSerializer( PdxSerializerPtr pdxSerializer )
  {
    SerializationRegistry::setPdxSerializer(pdxSerializer);
  }

  CacheableStringPtr Serializable::toString( ) const
  {
    return Utils::demangleTypeName(typeid(*this).name());
  }
}
