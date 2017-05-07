/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <cppcache/ExceptionTypes.hpp>
#include <cppcache/SignalHandler.hpp>
#include "impl/ManagedString.hpp"


using namespace System;
using namespace System::Collections::Generic;
using namespace System::Runtime::Serialization;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      ref class GemFireException;

      /// <summary>
      /// Factory delegate to create a managed GemFire exception.
      /// </summary>
      /// <remarks>
      /// For each managed exception class, its factory delegate is registered
      /// and maintained in a static dictionary mapped to its corresponding
      /// native GemFire C++ exception name.
      /// </remarks>

      delegate GemFireException^ CreateException(
        const gemfire::Exception& nativeEx, System::Exception^ innerException);

      /// <summary>
      /// The base exception class of all managed GemFire exceptions.
      /// </summary>
      [Serializable]
      //[Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class GemFireException
        : public System::Exception
      {
      private:

        /// <summary>
        /// Prefix for distiguishing managed system exceptions
        /// </summary>
        literal String^ MgSysExPrefix = "GFCLI_EXCEPTION:";

        /// <summary>
        /// This contains a mapping of the native GemFire exception class
        /// name to the factory delegate of the corresponding managed GemFire
        /// exception class.
        /// </summary>
        static Dictionary<String^, CreateException^>^ Native2ManagedExMap =
          Init( );

        /// <summary>
        /// Name and delegate pair class. The Native2ManagedExMap dictionary
        /// is populated from a static array of this class.
        /// </summary>
        value class NameDelegatePair
        {
        public:

          /// <summary>
          /// The name of the native GemFire exception class.
          /// </summary>
          String^ m_name;

          /// <summary>
          /// The factory delegate of the managed GemFire exception class
          /// corresponding to <c>m_name</c>
          /// </summary>
          CreateException^ m_delegate;
        };


      internal:

        /// <summary>
        /// Static method to associate the native exception names with
        /// the corresponding managed exception factory delegates.
        /// </summary>
        /// <remarks>
        /// This method is not thread-safe and should be called in a single thread.
        /// </remarks>
        static Dictionary<String^, CreateException^>^ Init( );

        /// <summary>
        /// Create the managed GemFire exception for a given native GemFire exception.
        /// As a special case normal system exceptions are also created when the
        /// native exception is a wrapper of a managed system exception.
        /// </summary>
        /// <remarks>
        /// Wherever the native GemFire C++ code raises a <c>gemfire::Exception</c>,
        /// the CLI wrapper code should have a catch-all for those and use
        /// this function to create the corresponding managed GemFire exception.
        /// If no managed GemFire exception has been defined (or has not been
        /// added using _GF_MG_EXCEPTION_ADD in ExceptionTypesM.cpp) then a
        /// generic <c>GemFireException</c> exception is returned.
        /// </remarks>
        /// <param name="nativeEx">The native GemFire exception object</param>
        /// <returns>
        /// The managed GemFire exception object corresponding to the provided
        /// native GemFire exception object.
        /// </returns>
        static Exception^ Get(const gemfire::Exception& nativeEx);

        /// <summary>
        /// Get the stack trace for the given native exception.
        /// </summary>
        /// <param name="nativeEx">The native GemFire exception object</param>
        /// <returns>The stack trace of the native exception.</returns>
        inline static String^ GetStackTrace(
          const gemfire::Exception& nativeEx )
        {
          char nativeExStack[2048] = { '\0' };
#ifndef _WIN64
          nativeEx.getStackTrace(nativeExStack, 2047);
#endif
					return GemStone::GemFire::ManagedString::Get(nativeExStack);
        }

        /// <summary>
        /// Gets the C++ native exception object for a given managed exception.
        /// </summary>
        /// <remarks>
        /// This method is to handle conversion of managed exceptions to
        /// C++ exception for those thrown by managed callbacks.
        /// For non-Gemfire .NET exceptions we wrap it inside the generic
        /// <c>GemfireException</c> with a special prefix in message.
        /// While converting the exception back from C++ to .NET if the
        /// prefix is found in the message, then it tries to construct
        /// the original exception by reflection on the name of exception
        /// contained in the message. Note that in this process the
        /// original stacktrace is appended to the message of the exception.
        /// </remarks>
        inline static gemfire::ExceptionPtr GetNative(Exception^ ex)
        {
          if (ex != nullptr) {
            GemFireException^ gfEx = dynamic_cast<GemFireException^>(ex);
            if (gfEx != nullptr) {
              return gfEx->GetNative();
            }
            else {
              gemfire::ExceptionPtr cause;
              if (ex->InnerException != nullptr) {
                cause = GemFireException::GetNative(ex->InnerException);
              }
              GemStone::GemFire::ManagedString mg_exStr(MgSysExPrefix + ex->ToString());
              return gemfire::ExceptionPtr(new gemfire::Exception(
                  mg_exStr.CharPtr, NULL, false, cause));
            }
          }
          return NULLPTR;
        }

        /// <summary>
        /// Gets the C++ native exception object for this managed
        /// <c>GemFireException</c>.
        /// </summary>
        virtual gemfire::ExceptionPtr GetNative()
        {
          String^ msg = this->Message + ": " + this->StackTrace;
          GemStone::GemFire::ManagedString mg_msg(msg);
          gemfire::ExceptionPtr cause;
          if (this->InnerException != nullptr) {
            cause = GemFireException::GetNative(this->InnerException);
          }
          return gemfire::ExceptionPtr(new gemfire::Exception(mg_msg.CharPtr,
              NULL, false, cause));
        }

        /// <summary>
        /// Throws the C++ native exception object for the given .NET exception.
        /// </summary>
        /// <remarks>
        /// This method is to handle conversion of managed exceptions to
        /// C++ exception for those thrown by managed callbacks.
        /// For non-Gemfire .NET exceptions we wrap it inside the generic
        /// <c>GemfireException</c> with a special prefix in message.
        /// While converting the exception back from C++ to .NET if the
        /// prefix is found in the message, then it tries to construct
        /// the original exception by reflection on the name of exception
        /// contained in the message. Note that in this process the
        /// original stacktrace is appended to the message of the exception.
        /// </remarks>
        inline static void ThrowNative(Exception^ ex)
        {
          if (ex != nullptr) {
            gemfire::ExceptionPtr cause;
            if (ex->InnerException != nullptr) {
              cause = GemFireException::GetNative(ex->InnerException);
            }
            GemStone::GemFire::ManagedString mg_exStr(MgSysExPrefix + ex->ToString());
            throw gemfire::Exception(mg_exStr.CharPtr, NULL, false, cause);
          }
        }

        /// <summary>
        /// Throws the C++ native exception object for this managed
        /// <c>GemFireException</c>.
        /// </summary>
        inline void ThrowNative()
        {
          GetNative()->raise();
        }


      public:

        /// <summary>
        /// Default constructor.
        /// </summary>
        inline GemFireException( )
          : Exception( ) { }

        /// <summary>
        /// Constructor to create an exception object with the given message.
        /// </summary>
        /// <param name="message">The exception message.</param>
        inline GemFireException( String^ message )
          : Exception( message ) { }

        /// <summary>
        /// Constructor to create an exception object with the given message
        /// and with the given inner exception.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="innerException">The inner exception object.</param>
        inline GemFireException( String^ message, System::Exception^ innerException )
          : Exception( message, innerException ) { }

        /// <summary>
        /// Generate a minidump of the current process in the directory
        /// specified for log files using "log-file" property.
        /// This is equivalent to the ".dump /ma" command of windbg.
        /// </summary>
        static String^ GenerateMiniDump();

        /// <summary>
        /// Generate a minidump of the current process in the directory
        /// specified for log files using "log-file" property.
        /// This is equivalent to the ".dump /ma" command of windbg.
        /// </summary>
        static String^ GenerateMiniDump(int32_t exceptionCode,
          IntPtr exceptionPointers);

      protected:

        /// <summary>
        /// Initializes a new instance of the <c>GemFireException</c> class with
        /// serialized data.
        /// This allows deserialization of this exception in .NET remoting.
        /// </summary>
        /// <param name="info">
        /// holds the serialized object data about
        /// the exception being thrown
        /// </param>
        /// <param name="context">
        /// contains contextual information about
        /// the source or destination
        /// </param>
        inline GemFireException( SerializationInfo^ info, StreamingContext context )
          : Exception( info, context ) { }
      };

/// Handle gemfire exceptions from native layer and convert to managed
/// exceptions. Also handle fatal exceptions to generate mini dumps if
/// stack trace dumping is enabled
#define _GF_MG_EXCEPTION_TRY        \
      try {
#define _GF_MG_EXCEPTION_CATCH_ALL  \
      } \
      catch (const gemfire::Exception& ex) { \
      throw GemStone::GemFire::Cache::GemFireException::Get(ex); \
      } \
      catch (System::AccessViolationException^ ex) { \
        GemStone::GemFire::Cache::GemFireException::GenerateMiniDump(); \
        throw ex; \
      }


/// Creates a class <c>x</c> named for each exception <c>y</c>.
#define _GF_MG_EXCEPTION_DEF2(x,y) \
      [Serializable] \
      public ref class x: public GemFireException \
      { \
      public: \
      \
        /** <summary>Default constructor</summary> */ \
        x( ) \
          : GemFireException( ) { } \
        \
        /** <summary>
         *  Constructor to create an exception object with the given message.
         *  </summary>
         *  <param name="message">The exception message.</param>
         */ \
        x( String^ message ) \
          : GemFireException( message ) { } \
        \
        /** <summary>
         *  Constructor to create an exception object with the given message
         *  and with the given inner exception.
         *  </summary>
         *  <param name="message">The exception message.</param>
         *  <param name="innerException">The inner exception object.</param>
         */ \
        x( String^ message, System::Exception^ innerException ) \
          : GemFireException( message, innerException ) { } \
        \
      protected: \
      \
        /** <summary>
         *  Initializes a new instance of the class with serialized data.
         *  This allows deserialization of this exception in .NET remoting.
         *  </summary>
         *  <param name="info">
         *  holds the serialized object data about the exception being thrown
         *  </param>
         *  <param name="context">
         *  contains contextual information about the source or destination
         *  </param>
         */ \
        x( SerializationInfo^ info, StreamingContext context ) \
          : GemFireException( info, context ) { } \
      \
      internal: \
        x(const gemfire::y& nativeEx) \
          : GemFireException(ManagedString::Get(nativeEx.getMessage()), \
              gcnew GemFireException(GemFireException::GetStackTrace( \
                nativeEx))) { } \
        \
        x(const gemfire::y& nativeEx, Exception^ innerException) \
          : GemFireException(ManagedString::Get(nativeEx.getMessage()), \
              innerException) { } \
        \
        static GemFireException^ Create(const gemfire::Exception& ex, \
            Exception^ innerException) \
        { \
          const gemfire::y* nativeEx = dynamic_cast<const gemfire::y*>( &ex ); \
          if (nativeEx != nullptr) { \
            if (innerException == nullptr) { \
              return gcnew x(*nativeEx); \
            } \
            else { \
              return gcnew x(*nativeEx, innerException); \
            } \
          } \
          return nullptr; \
        } \
        virtual gemfire::ExceptionPtr GetNative() override \
        { \
          String^ msg = this->Message + ": " + this->StackTrace; \
          ManagedString mg_msg(msg); \
          gemfire::ExceptionPtr cause; \
          if (this->InnerException != nullptr) { \
            cause = GemFireException::GetNative(this->InnerException); \
          } \
          return gemfire::ExceptionPtr(new gemfire::y(mg_msg.CharPtr, \
              NULL, false, cause)); \
        } \
      }

/// Creates a class named for each exception <c>x</c>.
#define _GF_MG_EXCEPTION_DEF(x) _GF_MG_EXCEPTION_DEF2(x,x)


      // For all the native GemFire C++ exceptions, a corresponding definition
      // should be added below *AND* it should also be added to the static array
      // in ExceptionTypesM.cpp using _GF_MG_EXCEPTION_ADD( x )

      /// <summary>
      /// A gemfire assertion exception.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( AssertionException );

      /// <summary>
      /// Thrown when an argument to a method is illegal.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( IllegalArgumentException );

      /// <summary>
      /// Thrown when the state of cache is manipulated to be illegal.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( IllegalStateException );

      /// <summary>
      /// Thrown when an attempt is made to create an existing cache.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CacheExistsException );

      /// <summary>
      /// Thrown when the cache xml is incorrect.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CacheXmlException );

      /// <summary>
      /// Thrown when a timout occurs.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( TimeoutException );

      /// <summary>
      /// Thrown when the cache writer aborts the operation.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CacheWriterException );

      /// <summary>
      /// Thrown when the cache listener throws an exception.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CacheListenerException );

      /// <summary>
      /// Thrown when an attempt is made to create an existing region.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( RegionExistsException );

      /// <summary>
      /// Thrown when an operation is attempted on a closed cache.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CacheClosedException );

      /// <summary>
      /// Thrown when lease of cache proxy has expired.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( LeaseExpiredException );

      /// <summary>
      /// Thrown when the cache loader aborts the operation.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CacheLoaderException );

      /// <summary>
      /// Thrown when an operation is attempted on a destroyed region.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( RegionDestroyedException );

      /// <summary>
      /// Thrown when an operation is attempted on a destroyed entry.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( EntryDestroyedException );

      /// <summary>
      /// Thrown when the connecting target is not running.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( NoSystemException );

      /// <summary>
      /// Thrown when an attempt is made to connect to
      /// DistributedSystem second time.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( AlreadyConnectedException );

      /// <summary>
      /// Thrown when a non-existing file is accessed.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( FileNotFoundException );

      /// <summary>
      /// Thrown when an operation is interrupted.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( InterruptedException );

      /// <summary>
      /// Thrown when an operation unsupported by the
      /// current configuration is attempted.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( UnsupportedOperationException );

      /// <summary>
      /// Thrown when statistics are invoked for a region where
      /// they are disabled.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( StatisticsDisabledException );

      /// <summary>
      /// Thrown when a concurrent operation fails.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( ConcurrentModificationException );

      /// <summary>
      /// An unknown exception occurred.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( UnknownException );

      /// <summary>
      /// Thrown when a cast operation fails.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( ClassCastException );

      /// <summary>
      /// Thrown when an operation is attempted on a non-existent entry.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( EntryNotFoundException );

      /// <summary>
      /// Thrown when there is an input/output error.
      /// </summary>
      _GF_MG_EXCEPTION_DEF2( GemFireIOException, GemfireIOException );

      /// <summary>
      /// Thrown when gemfire configuration file is incorrect.
      /// </summary>
      _GF_MG_EXCEPTION_DEF2( GemFireConfigException, GemfireConfigException );

      /// <summary>
      /// Thrown when a null argument is provided to a method
      /// where it is expected to be non-null.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( NullPointerException );

      /// <summary>
      /// Thrown when attempt is made to create an existing entry.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( EntryExistsException );

      /// <summary>
      /// Thrown when an operation is attempted before connecting
      /// to the distributed system.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( NotConnectedException );

      /// <summary>
      /// Thrown when there is an error in the cache proxy.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CacheProxyException );

      /// <summary>
      /// Thrown when the system cannot allocate any more memory.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( OutOfMemoryException );

      /// <summary>
      /// Thrown when an attempt is made to release a lock not
      /// owned by the thread.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( NotOwnerException );

      /// <summary>
      /// Thrown when a region is created in an incorrect scope.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( WrongRegionScopeException );

      /// <summary>
      /// Thrown when the internal buffer size is exceeded.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( BufferSizeExceededException );

      /// <summary>
      /// Thrown when a region creation operation fails.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( RegionCreationFailedException );

      /// <summary>
      /// Thrown when there is a fatal internal exception in GemFire.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( FatalInternalException );

      /// <summary>
      /// Thrown by the persistence manager when a write
      /// fails due to disk failure.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( DiskFailureException );

      /// <summary>
      /// Thrown by the persistence manager when the data
      /// to be read from disk is corrupt.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( DiskCorruptException );

      /// <summary>
      /// Thrown when persistence manager fails to initialize.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( InitFailedException );

      /// <summary>
      /// Thrown when persistence manager fails to close properly.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( ShutdownFailedException );

      /// <summary>
      /// Thrown when an exception occurs on the cache server.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CacheServerException );

      /// <summary>
      /// Thrown when bound of array/vector etc. is exceeded.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( OutOfRangeException );

      /// <summary>
      /// Thrown when query exception occurs at the server.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( QueryException );

      /// <summary>
      /// Thrown when an unknown message is received from the server.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( MessageException );

      /// <summary>
      /// Thrown when a client operation is not authorized on the server.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( NotAuthorizedException );

      /// <summary>
      /// Thrown when authentication to the server fails.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( AuthenticationFailedException );

      /// <summary>
      /// Thrown when credentials are not provided to a server which expects them.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( AuthenticationRequiredException );

      /// <summary>
      /// Thrown when a duplicate durable client id is provided to the server.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( DuplicateDurableClientException );

      /// <summary>
      /// Thrown when a client is unable to contact any locators.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( NoAvailableLocatorsException );

      /// <summary>
      /// Thrown when all connections in a pool are in use..
      /// </summary>
      _GF_MG_EXCEPTION_DEF( AllConnectionsInUseException );

      /// <summary>
      /// Thrown when cq is invalid
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CqInvalidException );

      /// <summary>
      /// Thrown when function execution failed
      /// </summary>
      _GF_MG_EXCEPTION_DEF( FunctionExecutionException );

      /// <summary>
      /// Thrown during continuous query execution time.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CqException );

      /// <summary>
      /// Thrown if the Cq on which the operaion performed is closed
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CqClosedException );

      /// <summary>
      /// Thrown if the Cq Query failed
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CqQueryException );

      /// <summary>
      /// Thrown if a Cq by this name already exists on this client
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CqExistsException );

      _GF_MG_EXCEPTION_DEF( InvalidDeltaException );

      /// <summary>
      /// Thrown if a Key is not present in the region.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( KeyNotFoundException );

      /// <summary>
      /// Thrown if commit fails.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( CommitConflictException );

	  
	        /// <summary>
      /// Thrown if transaction delegate went down.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( TransactionDataNodeHasDepartedException );

	        /// <summary>
      /// Thrown if commit rebalance happens during a transaction.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( TransactionDataRebalancedException );
      #ifdef CSTX_COMMENTED
      /// <summary>
      /// Thrown if transaction writer fails.
      /// </summary>
      _GF_MG_EXCEPTION_DEF( TransactionWriterException );
      #endif
    }
  }
}
