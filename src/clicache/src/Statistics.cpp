/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



//#include "gf_includes.hpp"
#include "Statistics.hpp"
#include "StatisticDescriptor.hpp"
#include "StatisticsType.hpp"

#include "impl/ManagedString.hpp"
#include "ExceptionTypes.hpp"
#include "impl/SafeConvert.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      void Statistics::Close()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          NativePtr->close();
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */ 
      }

      int32_t Statistics::NameToId(String^ name)
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->nameToId(mg_name.CharPtr);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */ 
      }

      StatisticDescriptor^ Statistics::NameToDescriptor(String^ name)
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return StatisticDescriptor::Create(NativePtr->nameToDescriptor(mg_name.CharPtr));
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */ 
      }

      int64_t Statistics::UniqueId::get( )
      {
        return NativePtr->getUniqueId();
      }

      StatisticsType^ Statistics::Type::get( )
      { 
        return StatisticsType::Create(NativePtr->getType());
      }

      String^ Statistics::TextId::get()
      {
        return ManagedString::Get(NativePtr->getTextId());
      }

      int64_t Statistics::NumericId::get()
      {
        return NativePtr->getNumericId();
      }
      bool Statistics::IsAtomic::get()
      {
        return NativePtr->isAtomic();
      }
      bool Statistics::IsShared::get()
      {
        return NativePtr->isShared();
      }
      bool Statistics::IsClosed::get()
      {
        return NativePtr->isClosed();
      }
      
      void Statistics::SetInt(int32_t id, int32_t value)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          NativePtr->setInt(id, value);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */ 
      } 

      void Statistics::SetInt(String^ name, int32_t value)
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          NativePtr->setInt((char*)mg_name.CharPtr, value);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */ 
      }

      void Statistics::SetInt(StatisticDescriptor^ descriptor, int32_t value)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          NativePtr->setInt(GetNativePtr<gemfire_statistics::StatisticDescriptor>(descriptor),value);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */ 
      }

      void Statistics::SetLong(int32_t id, int64_t value)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          NativePtr->setLong(id, value);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */ 
      }

      void Statistics::SetLong(StatisticDescriptor^ descriptor, int64_t value)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          NativePtr->setLong(GetNativePtr<gemfire_statistics::StatisticDescriptor>(descriptor),value);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */ 
      }

      void Statistics::SetLong(String^ name, int64_t value)
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          NativePtr->setLong((char*)mg_name.CharPtr, value);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */ 
      }

      void Statistics::SetDouble(int32_t id, double value)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          NativePtr->setDouble(id, value);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      void Statistics::SetDouble(String^ name, double value)
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          NativePtr->setDouble((char*)mg_name.CharPtr, value);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      void Statistics::SetDouble(StatisticDescriptor^ descriptor, double value)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
            NativePtr->setDouble(GetNativePtr<gemfire_statistics::StatisticDescriptor>(descriptor), value);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int32_t Statistics::GetInt(int32_t id)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->getInt(id);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int32_t Statistics::GetInt(StatisticDescriptor^ descriptor)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->getInt(GetNativePtr<gemfire_statistics::StatisticDescriptor>(descriptor));
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int32_t Statistics::GetInt(String^ name)
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->getInt((char*)mg_name.CharPtr);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int64_t Statistics::GetLong(int32_t id)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
           return NativePtr->getLong(id);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }
       int64_t Statistics::GetLong(StatisticDescriptor^ descriptor)
       {
          _GF_MG_EXCEPTION_TRY2/* due to auto replace */
            return NativePtr->getLong(GetNativePtr<gemfire_statistics::StatisticDescriptor>(descriptor));
          _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
       }

      int64_t Statistics::GetLong(String^ name)
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
         return NativePtr->getLong((char*)mg_name.CharPtr);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      double Statistics::GetDouble(int32_t id)
      {
         _GF_MG_EXCEPTION_TRY2/* due to auto replace */
           return NativePtr->getDouble(id);
         _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      double Statistics::GetDouble(StatisticDescriptor^ descriptor)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
           return NativePtr->getDouble(GetNativePtr<gemfire_statistics::StatisticDescriptor>(descriptor));
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      double Statistics::GetDouble(String^ name)
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->getDouble((char*)mg_name.CharPtr);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int64_t Statistics::GetRawBits(StatisticDescriptor^ descriptor)
      {
         _GF_MG_EXCEPTION_TRY2/* due to auto replace */
           return NativePtr->getRawBits(GetNativePtr<gemfire_statistics::StatisticDescriptor>(descriptor));
         _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int32_t Statistics::IncInt(int32_t id, int32_t delta)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->incInt(id,delta);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int32_t Statistics::IncInt(StatisticDescriptor^ descriptor, int32_t delta)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->incInt(GetNativePtr<gemfire_statistics::StatisticDescriptor>(descriptor),delta);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int32_t Statistics::IncInt(String^ name, int32_t delta)
      {
         ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->incInt((char*)mg_name.CharPtr,delta);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int64_t Statistics::IncLong(int32_t id, int64_t delta)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->incLong(id,delta);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int64_t Statistics::IncLong(StatisticDescriptor^ descriptor, int64_t delta)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->incLong(GetNativePtr<gemfire_statistics::StatisticDescriptor>(descriptor),delta);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      int64_t Statistics::IncLong(String^ name, int64_t delta)
      {
         ManagedString mg_name( name );
         _GF_MG_EXCEPTION_TRY2/* due to auto replace */
           return NativePtr->incLong((char*)mg_name.CharPtr,delta);
         _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      double Statistics::IncDouble(int32_t id, double delta)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->incDouble(id,delta);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      double Statistics::IncDouble(StatisticDescriptor^ descriptor, double delta)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->incDouble(GetNativePtr<gemfire_statistics::StatisticDescriptor>(descriptor),delta);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      double Statistics::IncDouble(String^ name, double delta)
      {
        ManagedString mg_name( name );
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->incDouble((char*)mg_name.CharPtr,delta);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }
    }
  }
} } //namespace 

