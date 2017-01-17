/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
