/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



//#include "gf_includes.hpp"
#include "StatisticDescriptor.hpp"
#include "impl/ManagedString.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      int32_t StatisticDescriptor::ID::get( )
      {
        return  NativePtr->getId();
      }

      String^ StatisticDescriptor::Name::get( )
      {
        return ManagedString::Get( NativePtr->getName() );
      }

      String^ StatisticDescriptor::Description::get( )
      {
        return ManagedString::Get( NativePtr->getDescription() );
      }

      int8_t StatisticDescriptor::IsCounter::get( )
      {
        return NativePtr->isCounter();
      }

      int8_t StatisticDescriptor::IsLargerBetter::get( )
      {
        return NativePtr->isLargerBetter();
      }

      String^ StatisticDescriptor::Unit::get( )
      {
        return ManagedString::Get( NativePtr->getUnit() );
      }
    }
  }
} } //namespace 

