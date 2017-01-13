/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "ExceptionTypes.hpp"
#include <stdlib.h>

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {

#define _GF_MG_EXCEPTION_ADD3(x) { "gemfire::" #x, gcnew CreateException2( x::Create ) }
#define _GF_MG_EXCEPTION_ADD4(x,y) { "gemfire::" #y, gcnew CreateException2( x::Create ) }

      Dictionary<String^, CreateException2^>^ GemFireException::Init( )
      {
        if (Native2ManagedExMap != nullptr)
        {
          return Native2ManagedExMap;
        }
        array<NameDelegatePair>^ exNamesDelegates = gcnew array<NameDelegatePair> {
          _GF_MG_EXCEPTION_ADD3( AssertionException ),
          _GF_MG_EXCEPTION_ADD3( IllegalArgumentException ),
          _GF_MG_EXCEPTION_ADD3( IllegalStateException ),
          _GF_MG_EXCEPTION_ADD3( CacheExistsException ),
          _GF_MG_EXCEPTION_ADD3( CacheXmlException ),
          _GF_MG_EXCEPTION_ADD3( TimeoutException ),
          _GF_MG_EXCEPTION_ADD3( CacheWriterException ),
          _GF_MG_EXCEPTION_ADD3( CacheListenerException ),
          _GF_MG_EXCEPTION_ADD3( RegionExistsException ),
          _GF_MG_EXCEPTION_ADD3( CacheClosedException ),
          _GF_MG_EXCEPTION_ADD3( LeaseExpiredException ),
          _GF_MG_EXCEPTION_ADD3( CacheLoaderException ),
          _GF_MG_EXCEPTION_ADD3( RegionDestroyedException ),
          _GF_MG_EXCEPTION_ADD3( EntryDestroyedException ),
          _GF_MG_EXCEPTION_ADD3( NoSystemException ),
          _GF_MG_EXCEPTION_ADD3( AlreadyConnectedException ),
          _GF_MG_EXCEPTION_ADD3( FileNotFoundException ),          
          _GF_MG_EXCEPTION_ADD3( InterruptedException ),
          _GF_MG_EXCEPTION_ADD3( UnsupportedOperationException ),
          _GF_MG_EXCEPTION_ADD3( StatisticsDisabledException ),
          _GF_MG_EXCEPTION_ADD3( ConcurrentModificationException ),
          _GF_MG_EXCEPTION_ADD3( UnknownException ),
          _GF_MG_EXCEPTION_ADD3( ClassCastException ),
          _GF_MG_EXCEPTION_ADD3( EntryNotFoundException ),
          _GF_MG_EXCEPTION_ADD4( GemFireIOException, GemfireIOException ),
          _GF_MG_EXCEPTION_ADD4( GemFireConfigException, GemfireConfigException ),
          _GF_MG_EXCEPTION_ADD3( NullPointerException ),
          _GF_MG_EXCEPTION_ADD3( EntryExistsException ),
          _GF_MG_EXCEPTION_ADD3( NotConnectedException ),
          _GF_MG_EXCEPTION_ADD3( CacheProxyException ),
          _GF_MG_EXCEPTION_ADD3( OutOfMemoryException ),
          _GF_MG_EXCEPTION_ADD3( NotOwnerException ),
          _GF_MG_EXCEPTION_ADD3( WrongRegionScopeException ),
          _GF_MG_EXCEPTION_ADD3( BufferSizeExceededException ),
          _GF_MG_EXCEPTION_ADD3( RegionCreationFailedException ),
          _GF_MG_EXCEPTION_ADD3( FatalInternalException ),
          _GF_MG_EXCEPTION_ADD3( DiskFailureException ),
          _GF_MG_EXCEPTION_ADD3( DiskCorruptException ),
          _GF_MG_EXCEPTION_ADD3( InitFailedException ),
          _GF_MG_EXCEPTION_ADD3( ShutdownFailedException ),
          _GF_MG_EXCEPTION_ADD3( CacheServerException ),
          _GF_MG_EXCEPTION_ADD3( OutOfRangeException ),
          _GF_MG_EXCEPTION_ADD3( QueryException ),
          _GF_MG_EXCEPTION_ADD3( MessageException ),
          _GF_MG_EXCEPTION_ADD3( NotAuthorizedException ),
          _GF_MG_EXCEPTION_ADD3( AuthenticationFailedException ),
          _GF_MG_EXCEPTION_ADD3( AuthenticationRequiredException ),
          _GF_MG_EXCEPTION_ADD3( DuplicateDurableClientException ),
          _GF_MG_EXCEPTION_ADD3( NoAvailableLocatorsException ),
          _GF_MG_EXCEPTION_ADD3( FunctionExecutionException ),
          _GF_MG_EXCEPTION_ADD3( CqInvalidException ),
          _GF_MG_EXCEPTION_ADD3( CqExistsException ),
          _GF_MG_EXCEPTION_ADD3( CqQueryException ),
          _GF_MG_EXCEPTION_ADD3( CqClosedException ),
          _GF_MG_EXCEPTION_ADD3( CqException ),
          _GF_MG_EXCEPTION_ADD3( AllConnectionsInUseException ),
          _GF_MG_EXCEPTION_ADD3( InvalidDeltaException ),
          _GF_MG_EXCEPTION_ADD3( KeyNotFoundException ),
          _GF_MG_EXCEPTION_ADD3( CommitConflictException ),
		  _GF_MG_EXCEPTION_ADD3( TransactionDataNodeHasDepartedException ),
		  _GF_MG_EXCEPTION_ADD3( TransactionDataRebalancedException )
        };

        Native2ManagedExMap = gcnew Dictionary<String^, CreateException2^>( );
        for (int32_t index = 0; index < exNamesDelegates->Length; index++)
        {
          Native2ManagedExMap[ exNamesDelegates[ index ].m_name ] =
            exNamesDelegates[ index ].m_delegate;
        }
        return Native2ManagedExMap;
      }

      System::Exception^ GemFireException::Get(const gemfire::Exception& nativeEx)
      {
        Exception^ innerException = nullptr;
        const gemfire::ExceptionPtr& cause = nativeEx.getCause();
        if (cause != NULLPTR) {
          innerException = GemFireException::Get(*cause);
        }
        String^ exName = gcnew String( nativeEx.getName( ) );
        CreateException2^ exDelegate;
        if (Native2ManagedExMap->TryGetValue(exName, exDelegate)) {
          return exDelegate(nativeEx, innerException);
        }
        String^ exMsg = ManagedString::Get( nativeEx.getMessage( ) );
        if ( exMsg->StartsWith( GemFireException::MgSysExPrefix ) ) {
          // Get the exception type
          String^ mgExStr = exMsg->Substring(
            GemFireException::MgSysExPrefix->Length );
          int32_t colonIndex = mgExStr->IndexOf( ':' );
          if ( colonIndex > 0 ) {
            String^ mgExName = mgExStr->Substring( 0, colonIndex )->Trim( );
            // Try to load this class by reflection
            Type^ mgExType = Type::GetType( mgExName, false, true );
            if ( mgExType != nullptr ) {
              System::Reflection::ConstructorInfo^ cInfo = mgExType->
                GetConstructor(gcnew array<Type^>{ String::typeid, Exception::typeid });
              if ( cInfo != nullptr ) {
                String^ mgMsg = mgExStr->Substring( colonIndex + 1 );
                Exception^ mgEx = dynamic_cast<Exception^>(cInfo->Invoke(
                      gcnew array<Object^>{ mgMsg, innerException }));
                if ( mgEx != nullptr ) {
                  return mgEx;
                }
              }
            }
          }
        }
        if (innerException == nullptr) {
          return gcnew GemFireException(exName + ": " + exMsg,
              gcnew GemFireException(GetStackTrace(nativeEx)));
        }
        else {
          return gcnew GemFireException(exName + ": " + exMsg, innerException);
        }
      }
      } // end namespace generic
    }
  }
}
