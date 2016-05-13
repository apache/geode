/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "QueryServiceM.hpp"
#include "QueryM.hpp"
#include "LogM.hpp"
#include "CqAttributesM.hpp"
#include "CqQueryM.hpp"
#include "CqServiceStatisticsM.hpp"
#include "impl/ManagedString.hpp"
#include "ExceptionTypesM.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      Query^ QueryService::NewQuery( String^ query )
      {
        ManagedString mg_queryStr( query );

        _GF_MG_EXCEPTION_TRY

          return Query::Create( NativePtr->newQuery(
            mg_queryStr.CharPtr ).ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }
      CqQuery^ QueryService::NewCq( String^ query, CqAttributes^ cqAttr, bool isDurable )
      {
        ManagedString mg_queryStr( query );
	gemfire::CqAttributesPtr attr(GetNativePtr<gemfire::CqAttributes>(cqAttr));
        try
        {
          return CqQuery::Create( NativePtr->newCq(
            mg_queryStr.CharPtr, attr, isDurable ).ptr( ));
        }
        catch ( const gemfire::Exception& ex )
        {
          throw GemFireException::Get( ex );
        }
      }
      CqQuery^ QueryService::NewCq( String^ name, String^ query, CqAttributes^ cqAttr, bool isDurable )
      {
        ManagedString mg_queryStr( query );
        ManagedString mg_nameStr( name );
	gemfire::CqAttributesPtr attr(GetNativePtr<gemfire::CqAttributes>(cqAttr));
        try
        {
          return CqQuery::Create( NativePtr->newCq(
            mg_nameStr.CharPtr, mg_queryStr.CharPtr, attr, isDurable ).ptr( ) );
        }
        catch ( const gemfire::Exception& ex )
        {
          throw GemFireException::Get( ex );
        }
      }
      void QueryService::CloseCqs()
      {
        try
        {
           NativePtr->closeCqs();
        }
        catch ( const gemfire::Exception& ex )
        {
          throw GemFireException::Get( ex );
        }
      }
      array<CqQuery^>^ QueryService::GetCqs()
      {
        try
        {
	   gemfire::VectorOfCqQuery vrr;
	   NativePtr->getCqs( vrr );
	   array<CqQuery^>^ cqs = gcnew array<CqQuery^>( vrr.size( ) );

           for( int32_t index = 0; index < vrr.size( ); index++ )
           {
                cqs[ index ] =  CqQuery::Create(vrr[index].ptr( ));
           }
           return cqs;
        }
        catch ( const gemfire::Exception& ex )
        {
          throw GemFireException::Get( ex );
        }
      }
      CqQuery^ QueryService::GetCq(String^ name)
      {
        ManagedString mg_queryStr( name );
        try
        {
          return CqQuery::Create( NativePtr->getCq(
            mg_queryStr.CharPtr ).ptr( ) );
        }
        catch ( const gemfire::Exception& ex )
        {
          throw GemFireException::Get( ex );
        }
      }
      void QueryService::ExecuteCqs()
      {
        try
        {
          NativePtr->executeCqs();
        }
        catch ( const gemfire::Exception& ex )
        {
          throw GemFireException::Get( ex );
        }
      }
      void QueryService::StopCqs()
      {
        try
        {
          NativePtr->stopCqs();
        }
        catch ( const gemfire::Exception& ex )
        {
          throw GemFireException::Get( ex );
        }
      }
      CqServiceStatistics^ QueryService::GetCqStatistics()
      {
        try
        {
          return CqServiceStatistics::Create( NativePtr->getCqServiceStatistics().ptr( ) );
        }
        catch ( const gemfire::Exception& ex )
        {
          throw GemFireException::Get( ex );
        }
      }

      System::Collections::Generic::List<String^>^ QueryService::GetAllDurableCqsFromServer()
      {
        try
        {
          gemfire::CacheableArrayListPtr durableCqsArrayListPtr = NativePtr->getAllDurableCqsFromServer();
          int length = durableCqsArrayListPtr != NULLPTR ? durableCqsArrayListPtr->length() : 0;
          System::Collections::Generic::List<String^>^ durableCqsList = gcnew System::Collections::Generic::List<String^>();
          if (length > 0)
          {
            for(int i =0; i < length; i++)
            {
              durableCqsList->Add(CacheableString::GetString(durableCqsArrayListPtr->at(i)));
            }
          }
          return durableCqsList;
        }
        catch ( const gemfire::Exception& ex )
        {
          throw GemFireException::Get( ex );
        }
      }
    }
  }
}
