/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



//#include "gf_includes.hpp"
#include "StatisticsType.hpp"
#include "StatisticDescriptor.hpp"

#include "impl/ManagedString.hpp"
#include "ExceptionTypes.hpp"
#include "impl/SafeConvert.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      String^ StatisticsType::Name::get()
      {
        return ManagedString::Get( NativePtr->getName() );
      }

      String^ StatisticsType::Description::get()
      {
        return ManagedString::Get( NativePtr->getDescription() );
      }

      array<StatisticDescriptor^>^ StatisticsType::Statistics::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire_statistics::StatisticDescriptor ** nativedescriptors = NativePtr->getStatistics();
          array<StatisticDescriptor^>^ descriptors = gcnew array<StatisticDescriptor^>(NativePtr->getDescriptorsCount());
          for (int item = 0; item < NativePtr->getDescriptorsCount(); item++)
          {
            descriptors[item] = StatisticDescriptor::Create(nativedescriptors[item]);
          }
          return descriptors;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int32_t StatisticsType::NameToId( String^ name )
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->nameToId(mg_name.CharPtr);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      StatisticDescriptor^ StatisticsType::NameToDescriptor( String^ name )
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return StatisticDescriptor::Create(NativePtr->nameToDescriptor(mg_name.CharPtr));
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int32_t StatisticsType::DescriptorsCount::get()
      {
        return NativePtr->getDescriptorsCount();
      }
    }
  }
}
 } //namespace 

