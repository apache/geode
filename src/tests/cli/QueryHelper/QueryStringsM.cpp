/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "QueryStringsM.hpp"
#include "impl/ManagedString.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Tests
      {

        // Region: QueryStrings method definitions

        void QueryStrings::Init( QueryCategory pcategory, String^ pquery,
          Boolean pisLargeResultset )
        {
          GemStone::GemFire::Cache::Generic::ManagedString mg_pquery( pquery );

          testData::QueryStrings* nativeptr = new testData::QueryStrings(
            static_cast<testData::queryCategory>( pcategory ),
            mg_pquery.CharPtr, pisLargeResultset );
          SetPtr( nativeptr, true );
        }

        Int32 QueryStrings::RSsize::get( )
        {
          return testData::QueryStrings::RSsize( );
        }

        Int32 QueryStrings::RSPsize::get( )
        {
          return testData::QueryStrings::RSPsize( );
        }

        Int32 QueryStrings::SSsize::get( )
        {
          return testData::QueryStrings::SSsize( );
        }

        Int32 QueryStrings::SSPsize::get( )
        {
          return testData::QueryStrings::SSPsize( );
        }

        Int32 QueryStrings::RQsize::get( )
        {
          return testData::QueryStrings::RQsize( );
        }

        Int32 QueryStrings::CQRSsize::get( )
        {
          return testData::QueryStrings::CQRSsize( );
        }  

        QueryCategory QueryStrings::Category::get()
        {
          return static_cast<QueryCategory>( NativePtr->category );
        }

        String^ QueryStrings::Query::get( )
        {
          return GemStone::GemFire::Cache::Generic::ManagedString::Get( NativePtr->query( ) );
        }

        bool QueryStrings::IsLargeResultset::get( )
        {
          return NativePtr->haveLargeResultset;
        }

        // End Region: QueryStrings method definitions


        // Region: QueryStatics method definitions

        static QueryStatics::QueryStatics( )
        {
          ResultSetQueries = gcnew array<QueryStrings^>(
            testData::RS_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::RS_ARRAY_SIZE; ++index) {
            ResultSetQueries[ index ] =
              gcnew QueryStrings( const_cast<testData::QueryStrings*>(
              &testData::resultsetQueries[ index ] ) );
          }

          ResultSetParamQueries = gcnew array<QueryStrings^>(
            testData::RSP_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::RSP_ARRAY_SIZE; ++index) {
            ResultSetParamQueries[ index ] =
              gcnew QueryStrings( const_cast<testData::QueryStrings*>(
              &testData::resultsetparamQueries[ index ] ) );
          }

          ResultSetRowCounts = gcnew array<Int32>( testData::RS_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::RS_ARRAY_SIZE; ++index) {
            ResultSetRowCounts[ index ] =
              testData::resultsetRowCounts[ index ];
          }

          ResultSetPQRowCounts = gcnew array<Int32>( testData::RSP_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::RSP_ARRAY_SIZE; ++index) {
            ResultSetPQRowCounts[ index ] =
              testData::resultsetRowCountsPQ[ index ];
          }

          NoOfQueryParam = gcnew array<Int32>( testData::RSP_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::RSP_ARRAY_SIZE; ++index) {
            NoOfQueryParam[ index ] =
              testData::noofQueryParam[ index ];
          }

          NoOfQueryParamSS = gcnew array<Int32>( testData::SSP_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::SSP_ARRAY_SIZE; ++index) {
            NoOfQueryParamSS[ index ] =
              testData::numSSQueryParam[ index ];
          }	

          QueryParamSet = gcnew array<array <String^>^>(testData::RSP_ARRAY_SIZE);
          for (Int32 index=0; index<testData::RSP_ARRAY_SIZE; index++)
          {
            QueryParamSet[index] = gcnew array<String^>(NoOfQueryParam[index]);
            for (Int32 index2=0; index2<NoOfQueryParam[index]; index2++)
            {
              QueryParamSet[index][index2] = gcnew String(testData::queryparamSet[index][index2]);

            }            
          }

          QueryParamSetSS = gcnew array<array <String^>^>(testData::SSP_ARRAY_SIZE);

          for (Int32 index=0; index<testData::SSP_ARRAY_SIZE; index++)
          {
            QueryParamSetSS[index] = gcnew array<String^>(NoOfQueryParamSS[index]);
            for (Int32 index2=0; index2 < NoOfQueryParamSS[index]; index2++)
            {
              QueryParamSetSS[index][index2] = gcnew String(testData::queryparamSetSS[index][index2]);

            }            
          }

          Int32 constantRowsRS = (Int32)(sizeof( testData::constantExpectedRowsRS )
            / sizeof( int ));
          ConstantExpectedRowsRS = gcnew array<Int32>( constantRowsRS );
          for (Int32 index = 0; index < constantRowsRS; ++index) {
            ConstantExpectedRowsRS[ index ] =
              testData::constantExpectedRowsRS[ index ];
          }

          Int32 constantRowsPQRS = (Int32)(sizeof( testData::constantExpectedRowsPQRS )
            / sizeof( int ));
          ConstantExpectedRowsPQRS = gcnew array<Int32>( constantRowsPQRS );
          for (Int32 index = 0; index < constantRowsPQRS; ++index) {
            ConstantExpectedRowsPQRS[ index ] =
              testData::constantExpectedRowsPQRS[ index ];
          }

          StructSetQueries = gcnew array<QueryStrings^>(
            testData::SS_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::SS_ARRAY_SIZE; ++index) {
            StructSetQueries[ index ] =
              gcnew QueryStrings( const_cast<testData::QueryStrings*>(
              &testData::structsetQueries[ index ] ) );
          }

          StructSetParamQueries = gcnew array<QueryStrings^>(
            testData::SSP_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::SSP_ARRAY_SIZE; ++index) {
            StructSetParamQueries[ index ] =
              gcnew QueryStrings( const_cast<testData::QueryStrings*>(
              &testData::structsetParamQueries[ index ] ) );
          }

          StructSetRowCounts = gcnew array<Int32>( testData::SS_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::SS_ARRAY_SIZE; ++index) {
            StructSetRowCounts[ index ] =
              testData::structsetRowCounts[ index ];
          }

          StructSetPQRowCounts = gcnew array<Int32>( testData::SSP_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::SSP_ARRAY_SIZE; ++index) {
            StructSetPQRowCounts[ index ] =
              testData::structsetRowCountsPQ[ index ];
          }

          StructSetFieldCounts = gcnew array<Int32>( testData::SS_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::SS_ARRAY_SIZE; ++index) {
            StructSetFieldCounts[ index ] =
              testData::structsetFieldCounts[ index ];
          }

          StructSetPQFieldCounts = gcnew array<Int32>( testData::SSP_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::SSP_ARRAY_SIZE; ++index) {
            StructSetPQFieldCounts[ index ] =
              testData::structsetFieldCountsPQ[ index ];
          }

          Int32 constantRowsSS = (Int32)(sizeof( testData::constantExpectedRowsSS )
            / sizeof( int ));
          ConstantExpectedRowsSS = gcnew array<Int32>( constantRowsSS );
          for (Int32 index = 0; index < constantRowsSS; ++index) {
            ConstantExpectedRowsSS[ index ] =
              testData::constantExpectedRowsSS[ index ];
          }

          Int32 constantRowsPQSS = (Int32)(sizeof( testData::constantExpectedRowsSSPQ )
            / sizeof( int ));
          ConstantExpectedRowsPQSS = gcnew array<Int32>( constantRowsPQSS );
          for (Int32 index = 0; index < constantRowsPQSS; ++index) {
            ConstantExpectedRowsPQSS[ index ] =
              testData::constantExpectedRowsSSPQ[ index ];
          }

          RegionQueries = gcnew array<QueryStrings^>(
            testData::RQ_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::RQ_ARRAY_SIZE; ++index) {
            RegionQueries[ index ] =
              gcnew QueryStrings( const_cast<testData::QueryStrings*>(
              &testData::regionQueries[ index ] ) );
          }

          RegionQueryRowCounts = gcnew array<Int32>( testData::RQ_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::RQ_ARRAY_SIZE; ++index) {
            RegionQueryRowCounts[ index ] =
              testData::regionQueryRowCounts[ index ];
          }

          CqResultSetQueries = gcnew array<QueryStrings^>(
            testData::CQRS_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::CQRS_ARRAY_SIZE; ++index) {
            CqResultSetQueries[ index ] =
              gcnew QueryStrings( const_cast<testData::QueryStrings*>(
              &testData::cqResultsetQueries[ index ] ) );
          }

          CqResultSetRowCounts = gcnew array<Int32>( testData::CQRS_ARRAY_SIZE );
          for (Int32 index = 0; index < testData::CQRS_ARRAY_SIZE; ++index) {
            CqResultSetRowCounts[ index ] =
              testData::cqResultsetRowCounts[ index ];
          }

          Int32 constantRowsCQRS = (Int32)(sizeof( testData::constantExpectedRowsCQRS )
            / sizeof( int ));
          ConstantExpectedRowsCQRS = gcnew array<Int32>( constantRowsCQRS );
          for (Int32 index = 0; index < constantRowsCQRS; ++index) {
            ConstantExpectedRowsCQRS[ index ] =
              testData::constantExpectedRowsCQRS[ index ];
          }

        }

        // End Region: QueryStatics method definitions

      }
    }
  }
}
