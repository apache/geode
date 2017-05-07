/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "QueryM.hpp"
#include "ISelectResults.hpp"
#include "ResultSetM.hpp"
#include "StructSetM.hpp"
#include "ExceptionTypesM.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      ISelectResults^ Query::Execute(  )
      {
        return Execute( DEFAULT_QUERY_RESPONSE_TIMEOUT );
      }

      ISelectResults^ Query::Execute( uint32_t timeout )
      {
        _GF_MG_EXCEPTION_TRY

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
            return StructSet::Create(structptr);
          }
          else
          {
            return ResultSet::Create(resultptr);
          }

        _GF_MG_EXCEPTION_CATCH_ALL
      }
	
      ISelectResults^ Query::Execute( array<IGFSerializable^>^ paramList)
      {
        return Execute(paramList, DEFAULT_QUERY_RESPONSE_TIMEOUT);
      }

      ISelectResults^ Query::Execute( array<IGFSerializable^>^ paramList, uint32_t timeout )
      {
        _GF_MG_EXCEPTION_TRY

        gemfire::CacheableVectorPtr rsptr = gemfire::CacheableVector::create();
        for( int index = 0; index < paramList->Length; index++ )
        {
		  rsptr->push_back(gemfire::CacheablePtr(SafeMSerializableConvert( paramList[ index])));
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
          return StructSet::Create(structptr);
        }
        else
        {
          return ResultSet::Create(resultptr);
        }

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      String^ Query::QueryString::get( )
      {
        return ManagedString::Get( NativePtr->getQueryString( ) );
      }

      void Query::Compile( )
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->compile( );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool Query::IsCompiled::get( )
      {
        _GF_MG_EXCEPTION_TRY

          return NativePtr->isCompiled( );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

    }
  }
}
