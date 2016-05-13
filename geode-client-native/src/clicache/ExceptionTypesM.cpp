/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include <cppcache/SignalHandler.hpp>
#include "ExceptionTypesM.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

#define _GF_MG_EXCEPTION_ADD(x) { "gemfire::" #x, gcnew CreateException( x::Create ) }
#define _GF_MG_EXCEPTION_ADD2(x,y) { "gemfire::" #y, gcnew CreateException( x::Create ) }

      Dictionary<String^, CreateException^>^ GemFireException::Init( )
      {
        if (Native2ManagedExMap != nullptr)
        {
          return Native2ManagedExMap;
        }
        array<NameDelegatePair>^ exNamesDelegates = gcnew array<NameDelegatePair> {
          _GF_MG_EXCEPTION_ADD( AssertionException ),
          _GF_MG_EXCEPTION_ADD( IllegalArgumentException ),
          _GF_MG_EXCEPTION_ADD( IllegalStateException ),
          _GF_MG_EXCEPTION_ADD( CacheExistsException ),
          _GF_MG_EXCEPTION_ADD( CacheXmlException ),
          _GF_MG_EXCEPTION_ADD( TimeoutException ),
          _GF_MG_EXCEPTION_ADD( CacheWriterException ),
          _GF_MG_EXCEPTION_ADD( CacheListenerException ),
          _GF_MG_EXCEPTION_ADD( RegionExistsException ),
          _GF_MG_EXCEPTION_ADD( CacheClosedException ),
          _GF_MG_EXCEPTION_ADD( LeaseExpiredException ),
          _GF_MG_EXCEPTION_ADD( CacheLoaderException ),
          _GF_MG_EXCEPTION_ADD( RegionDestroyedException ),
          _GF_MG_EXCEPTION_ADD( EntryDestroyedException ),
          _GF_MG_EXCEPTION_ADD( NoSystemException ),
          _GF_MG_EXCEPTION_ADD( AlreadyConnectedException ),
          _GF_MG_EXCEPTION_ADD( FileNotFoundException ),          
          _GF_MG_EXCEPTION_ADD( InterruptedException ),
          _GF_MG_EXCEPTION_ADD( UnsupportedOperationException ),
          _GF_MG_EXCEPTION_ADD( StatisticsDisabledException ),
          _GF_MG_EXCEPTION_ADD( ConcurrentModificationException ),
          _GF_MG_EXCEPTION_ADD( UnknownException ),
          _GF_MG_EXCEPTION_ADD( ClassCastException ),
          _GF_MG_EXCEPTION_ADD( EntryNotFoundException ),
          _GF_MG_EXCEPTION_ADD2( GemFireIOException, GemfireIOException ),
          _GF_MG_EXCEPTION_ADD2( GemFireConfigException, GemfireConfigException ),
          _GF_MG_EXCEPTION_ADD( NullPointerException ),
          _GF_MG_EXCEPTION_ADD( EntryExistsException ),
          _GF_MG_EXCEPTION_ADD( NotConnectedException ),
          _GF_MG_EXCEPTION_ADD( CacheProxyException ),
          _GF_MG_EXCEPTION_ADD( OutOfMemoryException ),
          _GF_MG_EXCEPTION_ADD( NotOwnerException ),
          _GF_MG_EXCEPTION_ADD( WrongRegionScopeException ),
          _GF_MG_EXCEPTION_ADD( BufferSizeExceededException ),
          _GF_MG_EXCEPTION_ADD( RegionCreationFailedException ),
          _GF_MG_EXCEPTION_ADD( FatalInternalException ),
          _GF_MG_EXCEPTION_ADD( DiskFailureException ),
          _GF_MG_EXCEPTION_ADD( DiskCorruptException ),
          _GF_MG_EXCEPTION_ADD( InitFailedException ),
          _GF_MG_EXCEPTION_ADD( ShutdownFailedException ),
          _GF_MG_EXCEPTION_ADD( CacheServerException ),
          _GF_MG_EXCEPTION_ADD( OutOfRangeException ),
          _GF_MG_EXCEPTION_ADD( QueryException ),
          _GF_MG_EXCEPTION_ADD( MessageException ),
          _GF_MG_EXCEPTION_ADD( NotAuthorizedException ),
          _GF_MG_EXCEPTION_ADD( AuthenticationFailedException ),
          _GF_MG_EXCEPTION_ADD( AuthenticationRequiredException ),
          _GF_MG_EXCEPTION_ADD( DuplicateDurableClientException ),
          _GF_MG_EXCEPTION_ADD( NoAvailableLocatorsException ),
          _GF_MG_EXCEPTION_ADD( FunctionExecutionException ),
          _GF_MG_EXCEPTION_ADD( CqInvalidException ),
          _GF_MG_EXCEPTION_ADD( CqExistsException ),
          _GF_MG_EXCEPTION_ADD( CqQueryException ),
          _GF_MG_EXCEPTION_ADD( CqClosedException ),
          _GF_MG_EXCEPTION_ADD( CqException ),
          _GF_MG_EXCEPTION_ADD( AllConnectionsInUseException ),
          _GF_MG_EXCEPTION_ADD( InvalidDeltaException ),
          _GF_MG_EXCEPTION_ADD( KeyNotFoundException ),
          _GF_MG_EXCEPTION_ADD( CommitConflictException ),
		  _GF_MG_EXCEPTION_ADD( TransactionDataNodeHasDepartedException ),
		  _GF_MG_EXCEPTION_ADD( TransactionDataRebalancedException )

          #ifdef CSTX_COMMENTED
          ,_GF_MG_EXCEPTION_ADD( TransactionWriterException )
          #endif
        };

        Native2ManagedExMap = gcnew Dictionary<String^, CreateException^>( );
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
        CreateException^ exDelegate;
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

      String^ GemFireException::GenerateMiniDump()
      {
        char dumpFile[_MAX_PATH];
        gemfire::SignalHandler::dumpStack(dumpFile, _MAX_PATH - 1);
        return ManagedString::Get(dumpFile);
      }

      String^ GemFireException::GenerateMiniDump(int32_t exceptionCode,
        IntPtr exceptionPointers)
      {
        char dumpFile[_MAX_PATH];
        gemfire::SignalHandler::dumpStack((unsigned int)exceptionCode,
          (EXCEPTION_POINTERS*)(void*)exceptionPointers, dumpFile,
          _MAX_PATH - 1);
        return ManagedString::Get(dumpFile);
      }

    }
  }
}
