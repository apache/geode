/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "Query.hpp"
#include "ISelectResults.hpp"
#include "ResultSet.hpp"
#include "StructSet.hpp"
#include "ExceptionTypes.hpp"
//#include "Serializable.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TResult>
      ISelectResults<TResult>^ Query<TResult>::Execute(  )
      {
        return Execute( DEFAULT_QUERY_RESPONSE_TIMEOUT );
      }

      generic<class TResult>
      ISelectResults<TResult>^ Query<TResult>::Execute( uint32_t timeout )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::SelectResultsPtr& nativeptr = NativePtr->execute( timeout );
          if ( nativeptr.ptr( ) == NULL ) return nullptr;

          gemfire::ResultSet* resultptr = dynamic_cast<gemfire::ResultSet*>(
            nativeptr.ptr( ) );
          if ( resultptr == NULL )
          {
            gemfire::StructSet* structptr = dynamic_cast<gemfire::StructSet*>(
              nativeptr.ptr( ) );
            if ( structptr == NULL )
            {
              return nullptr;
            }
            return StructSet<TResult>::Create(structptr);
          }
          else
          {
            return ResultSet<TResult>::Create(resultptr);
          }

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }
	
      generic<class TResult>
      ISelectResults<TResult>^ Query<TResult>::Execute( array<Object^>^ paramList)
      {
        return Execute(paramList, DEFAULT_QUERY_RESPONSE_TIMEOUT);
      }

      generic<class TResult>
      ISelectResults<TResult>^ Query<TResult>::Execute( array<Object^>^ paramList, uint32_t timeout )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        gemfire::CacheableVectorPtr rsptr = gemfire::CacheableVector::create();
        for( int index = 0; index < paramList->Length; index++ )
        {
          gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<Object^>(paramList[index]->GetType(), (Object^)paramList[index]) ) ;
          rsptr->push_back(valueptr);
		    }
        
        gemfire::SelectResultsPtr& nativeptr = NativePtr->execute(rsptr, timeout );
        if ( nativeptr.ptr( ) == NULL ) return nullptr;

        gemfire::ResultSet* resultptr = dynamic_cast<gemfire::ResultSet*>(
        nativeptr.ptr( ) );
        if ( resultptr == NULL )
        {
          gemfire::StructSet* structptr = dynamic_cast<gemfire::StructSet*>(
          nativeptr.ptr( ) );
          if ( structptr == NULL )
          {
            return nullptr;
          }
          return StructSet<TResult>::Create(structptr);
        }
        else
        {
          return ResultSet<TResult>::Create(resultptr);
        }

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      String^ Query<TResult>::QueryString::get( )
      {
        return ManagedString::Get( NativePtr->getQueryString( ) );
      }

      generic<class TResult>
      void Query<TResult>::Compile( )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->compile( );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      bool Query<TResult>::IsCompiled::get( )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          return NativePtr->isCompiled( );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

    }
  }
}
 } //namespace 
