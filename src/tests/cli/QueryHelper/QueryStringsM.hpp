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

#pragma once

#include "QueryStrings.hpp"
#include "impl/NativeWrapper.hpp"


using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Tests
      {

        /// <summary>
        /// Specifies the category of a query
        /// </summary>
        public enum class QueryCategory
        {
          SingleRegion = 0,
          MultiRegion,
          Operators,
          Constants,
          Functions,
          CollectionOps,
          Keywords,
          RegionInterface,
          NestedQueries,
          ImportAndSelect,
          Canonicalization,
          Unsupported,
          QueryAndIndexing,
          Misc,
          RegionQuery,
          QcEnd // to mark the end of enum
        };


        /// <summary>
        /// Encapsulates a query string.
        /// </summary>
        public ref class QueryStrings sealed
          : public Apache::Geode::Client::Internal::UMWrap<testData::QueryStrings>
        {
        public:

          inline QueryStrings( QueryCategory pcategory, String^ pquery,
            Boolean pisLargeResultset )
          {
            Init( pcategory, pquery, pisLargeResultset );
          }

          QueryStrings( QueryCategory pcategory, String^ pquery )
          {
            Init( pcategory, pquery, false );
          }

          static property Int32 RSsize
          {
            Int32 get( );
          }

          static property Int32 RSPsize
          {
            Int32 get( );
          }

          static property Int32 SSsize
          {
            Int32 get( );
          }

          static property Int32 SSPsize
          {
            Int32 get( );
          }

          static property Int32 RQsize
          {
            Int32 get( );
          }

          static property Int32 CQRSsize
          {
            Int32 get( );
          }

          property QueryCategory Category
          {
            QueryCategory get( );
          }

          property String^ Query
          {
            String^ get( );
          }

          property bool IsLargeResultset
          {
            bool get( );
          }

        private:

          void Init( QueryCategory pcategory, String^ pquery,
            Boolean pisLargeResultset );


        internal:

          /// <summary>
          /// Internal constructor to wrap a native object pointer
          /// </summary>
          /// <param name="nativeptr">The native object pointer</param>
          inline QueryStrings( testData::QueryStrings* nativeptr )
            : UMWrap( nativeptr, false ) { }
        };

        /// <summary>
        /// Contains static query arrays and their expected results.
        /// </summary>
        public ref class QueryStatics sealed
        {
        public:

          static array<QueryStrings^>^ ResultSetQueries;

          static array<QueryStrings^>^ ResultSetParamQueries;

          static array<array<String^>^>^ QueryParamSet;

          static array<array<String^>^>^ QueryParamSetSS;

          static array<Int32>^ NoOfQueryParam;

          static array<Int32>^ NoOfQueryParamSS;

          static array<Int32>^ ResultSetRowCounts;

          static array<Int32>^ ResultSetPQRowCounts;

          static array<Int32>^ ConstantExpectedRowsRS;

          static array<Int32>^ ConstantExpectedRowsPQRS;

          static array<QueryStrings^>^ StructSetQueries;

          static array<QueryStrings^>^ StructSetParamQueries;

          static array<Int32>^ StructSetRowCounts;

          static array<Int32>^ StructSetPQRowCounts;

          static array<Int32>^ StructSetFieldCounts;

          static array<Int32>^ StructSetPQFieldCounts;

          static array<Int32>^ ConstantExpectedRowsSS;

          static array<Int32>^ ConstantExpectedRowsPQSS;

          static array<QueryStrings^>^ RegionQueries;

          static array<Int32>^ RegionQueryRowCounts;

          static array<QueryStrings^>^ CqResultSetQueries;

          static array<Int32>^ CqResultSetRowCounts;

          static array<Int32>^ ConstantExpectedRowsCQRS;

        private:

          static QueryStatics( );
        };

      }
    }
  }
}
