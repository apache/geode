/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "StatisticsFactoryM.hpp"
#include "StatisticsTypeM.hpp"
#include "StatisticDescriptorM.hpp"
#include "StatisticsM.hpp"
#include "impl/SafeConvert.hpp"
#include "ExceptionTypesM.hpp"

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      StatisticsFactory^ StatisticsFactory::GetExistingInstance()
      {
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::StatisticsFactory::Create(gemfire_statistics::StatisticsFactory::getExistingInstance());

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      StatisticDescriptor^ StatisticsFactory::CreateIntCounter( String^ name, String^ description,String^ units )
      {
        return CreateIntCounter(name,description,units,true);
      }

      StatisticDescriptor^ StatisticsFactory::CreateIntCounter(String^ name, String^ description,String^ units, int8_t largerBetter)
      {
        ManagedString mg_name( name );
        ManagedString mg_description( description );
        ManagedString mg_units( units );
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::StatisticDescriptor::Create(NativePtr->createIntCounter(mg_name.CharPtr, mg_description.CharPtr, mg_units.CharPtr, largerBetter));

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      StatisticDescriptor^ StatisticsFactory::CreateLongCounter( String^ name, String^ description,String^ units )
      {
        return CreateLongCounter(name,description,units,true);
      }

      StatisticDescriptor^ StatisticsFactory::CreateLongCounter( String^ name, String^ description,String^ units, int8_t largerBetter )
      {
        ManagedString mg_name( name );
        ManagedString mg_description( description );
        ManagedString mg_units( units );
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::StatisticDescriptor::Create(NativePtr->createLongCounter(mg_name.CharPtr, mg_description.CharPtr, mg_units.CharPtr, largerBetter));

        _GF_MG_EXCEPTION_CATCH_ALL
      }      
        
      StatisticDescriptor^ StatisticsFactory::CreateDoubleCounter( String^ name, String^ description, String^ units )
      {
        return CreateDoubleCounter(name,description,units,true);
      }

      StatisticDescriptor^ StatisticsFactory::CreateDoubleCounter( String^ name, String^ description, String^ units, int8_t largerBetter )
      {
        ManagedString mg_name( name );
        ManagedString mg_description( description );
        ManagedString mg_units( units );
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::StatisticDescriptor::Create(NativePtr->createDoubleCounter(mg_name.CharPtr, mg_description.CharPtr, mg_units.CharPtr, largerBetter));

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      
      StatisticDescriptor^ StatisticsFactory::CreateIntGauge( String^ name, String^ description, String^ units )
      {
        return CreateIntGauge(name,description,units,false);
      }

      StatisticDescriptor^ StatisticsFactory::CreateIntGauge( String^ name, String^ description, String^ units, int8_t largerBetter )
      {
        ManagedString mg_name( name );
        ManagedString mg_description( description );
        ManagedString mg_units( units );
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::StatisticDescriptor::Create(NativePtr->createIntGauge(mg_name.CharPtr, mg_description.CharPtr, mg_units.CharPtr, largerBetter));

        _GF_MG_EXCEPTION_CATCH_ALL      
      }

      StatisticDescriptor^ StatisticsFactory::CreateLongGauge( String^ name, String^ description, String^ units )
      {
        return CreateLongGauge(name,description,units,false);
      }

      StatisticDescriptor^ StatisticsFactory::CreateLongGauge( String^ name, String^ description, String^ units, int8_t largerBetter )
      {
        ManagedString mg_name( name );
        ManagedString mg_description( description );
        ManagedString mg_units( units );
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::StatisticDescriptor::Create(NativePtr->createLongGauge(mg_name.CharPtr, mg_description.CharPtr, mg_units.CharPtr, largerBetter));

        _GF_MG_EXCEPTION_CATCH_ALL      
      }
      
      StatisticDescriptor^ StatisticsFactory::CreateDoubleGauge( String^ name, String^ description, String^ units )
      {
        return CreateDoubleGauge(name,description,units,false);
      }

      StatisticDescriptor^ StatisticsFactory::CreateDoubleGauge( String^ name, String^ description, String^ units,int8_t largerBetter )
      {
        ManagedString mg_name( name );
        ManagedString mg_description( description );
        ManagedString mg_units( units );
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::StatisticDescriptor::Create(NativePtr->createDoubleGauge(mg_name.CharPtr, mg_description.CharPtr, mg_units.CharPtr, largerBetter));

        _GF_MG_EXCEPTION_CATCH_ALL      
      }

      StatisticsType^ StatisticsFactory::CreateType( String^ name, String^ description,
                                   array<StatisticDescriptor^>^ stats, int32 statsLength)
      {
        ManagedString mg_name( name );
        ManagedString mg_description( description );
        _GF_MG_EXCEPTION_TRY
                
          gemfire_statistics::StatisticDescriptor ** nativedescriptors = new gemfire_statistics::StatisticDescriptor*[statsLength];
          for (int32_t index = 0; index < statsLength; index++)
          {
            nativedescriptors[index] = GetNativePtr<gemfire_statistics::StatisticDescriptor>(stats[index]);
          }
          return GemStone::GemFire::Cache::StatisticsType::Create(NativePtr->createType(mg_name.CharPtr, mg_description.CharPtr, nativedescriptors, statsLength));
          
        _GF_MG_EXCEPTION_CATCH_ALL     
      }

      StatisticsType^ StatisticsFactory::FindType(String^ name)
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::StatisticsType::Create(NativePtr->findType(mg_name.CharPtr));

        _GF_MG_EXCEPTION_CATCH_ALL     
      }

      Statistics^ StatisticsFactory::CreateStatistics(StatisticsType^ type)
      {
        _GF_MG_EXCEPTION_TRY
         
          return GemStone::GemFire::Cache::Statistics::Create(NativePtr->createStatistics(GetNativePtr<gemfire_statistics::StatisticsType>(type)));

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Statistics^ StatisticsFactory::CreateStatistics(StatisticsType^ type, String^ textId)
      {
        ManagedString mg_text( textId );
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::Statistics::Create(NativePtr->createStatistics(GetNativePtr<gemfire_statistics::StatisticsType>(type),(char*)mg_text.CharPtr));

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Statistics^ StatisticsFactory::CreateStatistics(StatisticsType^ type, String^ textId, int64_t numericId)
      {
        ManagedString mg_text( textId );
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::Statistics::Create(NativePtr->createStatistics(GetNativePtr<gemfire_statistics::StatisticsType>(type),(char*)mg_text.CharPtr, numericId));

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Statistics^ StatisticsFactory::CreateAtomicStatistics(StatisticsType^ type)
      {
        _GF_MG_EXCEPTION_TRY
         
          return GemStone::GemFire::Cache::Statistics::Create(NativePtr->createAtomicStatistics(GetNativePtr<gemfire_statistics::StatisticsType>(type)));

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Statistics^ StatisticsFactory::CreateAtomicStatistics(StatisticsType^ type, String^ textId)
      {
        ManagedString mg_text( textId );
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::Statistics::Create(NativePtr->createAtomicStatistics(GetNativePtr<gemfire_statistics::StatisticsType>(type),(char*)mg_text.CharPtr));

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Statistics^ StatisticsFactory::CreateAtomicStatistics(StatisticsType^ type, String^ textId, int64_t numericId)
      {
        ManagedString mg_text( textId );
        _GF_MG_EXCEPTION_TRY

          return GemStone::GemFire::Cache::Statistics::Create(NativePtr->createAtomicStatistics(GetNativePtr<gemfire_statistics::StatisticsType>(type),(char*)mg_text.CharPtr, numericId));

        _GF_MG_EXCEPTION_CATCH_ALL
      }
      Statistics^ StatisticsFactory::FindFirstStatisticsByType( StatisticsType^ type )
      {
        _GF_MG_EXCEPTION_TRY
         
          return GemStone::GemFire::Cache::Statistics::Create(NativePtr->findFirstStatisticsByType(GetNativePtr<gemfire_statistics::StatisticsType>(type)));

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      String^ StatisticsFactory::Name::get( )
      {
        return ManagedString::Get( NativePtr->getName() );
      }

      int64_t StatisticsFactory::ID::get( )
      {
        return  NativePtr->getId();
      }
    }
  }
}

