/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "QueryService.hpp"
#include "Query.hpp"
#include "Log.hpp"
#include "CqAttributes.hpp"
#include "CqQuery.hpp"
#include "CqServiceStatistics.hpp"
#include "impl/ManagedString.hpp"
#include "ExceptionTypes.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TKey, class TResult>
      //generic<class TResult>
      Query<TResult>^ QueryService<TKey, TResult>::NewQuery( String^ query )
      {
        ManagedString mg_queryStr( query );

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          return Query<TResult>::Create( NativePtr->newQuery(
            mg_queryStr.CharPtr ).ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TResult>
      CqQuery<TKey, TResult>^ QueryService<TKey, TResult>::NewCq( String^ query, CqAttributes<TKey, TResult>^ cqAttr, bool isDurable )
      {
        ManagedString mg_queryStr( query );
	gemfire::CqAttributesPtr attr(GetNativePtrFromSBWrapGeneric<gemfire::CqAttributes>(cqAttr));
        try
        {
          return CqQuery<TKey, TResult>::Create( NativePtr->newCq(
            mg_queryStr.CharPtr, attr, isDurable ).ptr( ));
        }
        catch ( const gemfire::Exception& ex )
        {
          throw GemFireException::Get( ex );
        }
      }

      generic<class TKey, class TResult>
      CqQuery<TKey, TResult>^ QueryService<TKey, TResult>::NewCq( String^ name, String^ query, CqAttributes<TKey, TResult>^ cqAttr, bool isDurable )
      {
        ManagedString mg_queryStr( query );
        ManagedString mg_nameStr( name );
	gemfire::CqAttributesPtr attr(GetNativePtrFromSBWrapGeneric<gemfire::CqAttributes>(cqAttr));
        try
        {
          return CqQuery<TKey, TResult>::Create( NativePtr->newCq(
            mg_nameStr.CharPtr, mg_queryStr.CharPtr, attr, isDurable ).ptr( ) );
        }
        catch ( const gemfire::Exception& ex )
        {
          throw GemFireException::Get( ex );
        }
      }

      generic<class TKey, class TResult>
      void QueryService<TKey, TResult>::CloseCqs()
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

      generic<class TKey, class TResult>
      array<CqQuery<TKey, TResult>^>^ QueryService<TKey, TResult>::GetCqs()
      {
        try
        {
	   gemfire::VectorOfCqQuery vrr;
	   NativePtr->getCqs( vrr );
	   array<CqQuery<TKey, TResult>^>^ cqs = gcnew array<CqQuery<TKey, TResult>^>( vrr.size( ) );

           for( int32_t index = 0; index < vrr.size( ); index++ )
           {
                cqs[ index ] =  CqQuery<TKey, TResult>::Create(vrr[index].ptr( ));
           }
           return cqs;
        }
        catch ( const gemfire::Exception& ex )
        {
          throw GemFireException::Get( ex );
        }
      }

      generic<class TKey, class TResult>
      CqQuery<TKey, TResult>^ QueryService<TKey, TResult>::GetCq(String^ name)
      {
        ManagedString mg_queryStr( name );
        try
        {
          return CqQuery<TKey, TResult>::Create( NativePtr->getCq(
            mg_queryStr.CharPtr ).ptr( ) );
        }
        catch ( const gemfire::Exception& ex )
        {
          throw GemFireException::Get( ex );
        }
      }

      generic<class TKey, class TResult>
      void QueryService<TKey, TResult>::ExecuteCqs()
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

      generic<class TKey, class TResult>
      void QueryService<TKey, TResult>::StopCqs()
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

      generic<class TKey, class TResult>
      CqServiceStatistics^ QueryService<TKey, TResult>::GetCqStatistics()
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

      generic<class TKey, class TResult>
      System::Collections::Generic::List<String^>^ QueryService<TKey, TResult>::GetAllDurableCqsFromServer()
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
 } //namespace 
