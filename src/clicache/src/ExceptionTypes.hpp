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

#include "gf_defs.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include "impl/ManagedString.hpp"


using namespace System;
using namespace System::Collections::Generic;
using namespace System::Runtime::Serialization;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      ref class GeodeException;

      /// <summary>
      /// Factory delegate to create a managed Geode exception.
      /// </summary>
      /// <remarks>
      /// For each managed exception class, its factory delegate is registered
      /// and maintained in a static dictionary mapped to its corresponding
      /// native Geode C++ exception name.
      /// </remarks>
      delegate GeodeException^ CreateException2(
        const apache::geode::client::Exception& nativeEx, System::Exception^ innerException);

      /// <summary>
      /// The base exception class of all managed Geode exceptions.
      /// </summary>
      [Serializable]
      public ref class GeodeException
        : public System::Exception
      {
      private:

        /// <summary>
        /// Prefix for distiguishing managed system exceptions
        /// </summary>
        literal String^ MgSysExPrefix = "GFCLI_EXCEPTION:";

        /// <summary>
        /// This contains a mapping of the native Geode exception class
        /// name to the factory delegate of the corresponding managed Geode
        /// exception class.
        /// </summary>
        static Dictionary<String^, CreateException2^>^ Native2ManagedExMap =
          Init( );

        /// <summary>
        /// Name and delegate pair class. The Native2ManagedExMap dictionary
        /// is populated from a static array of this class.
        /// </summary>
        value class NameDelegatePair
        {
        public:

          /// <summary>
          /// The name of the native Geode exception class.
          /// </summary>
          String^ m_name;

          /// <summary>
          /// The factory delegate of the managed Geode exception class
          /// corresponding to <c>m_name</c>
          /// </summary>
          CreateException2^ m_delegate;
        };


      internal:

        /// <summary>
        /// Static method to associate the native exception names with
        /// the corresponding managed exception factory delegates.
        /// </summary>
        /// <remarks>
        /// This method is not thread-safe and should be called in a single thread.
        /// </remarks>
        static Dictionary<String^, CreateException2^>^ Init( );

        /// <summary>
        /// Create the managed Geode exception for a given native Geode exception.
        /// As a special case normal system exceptions are also created when the
        /// native exception is a wrapper of a managed system exception.
        /// </summary>
        /// <remarks>
        /// Wherever the native Geode C++ code raises a <c>apache::geode::client::Exception</c>,
        /// the CLI wrapper code should have a catch-all for those and use
        /// this function to create the corresponding managed Geode exception.
        /// If no managed Geode exception has been defined (or has not been
        /// added using _GF_MG_EXCEPTION_ADD3 in ExceptionTypesMN.cpp) then a
        /// generic <c>GeodeException</c> exception is returned.
        /// </remarks>
        /// <param name="nativeEx">The native Geode exception object</param>
        /// <returns>
        /// The managed Geode exception object corresponding to the provided
        /// native Geode exception object.
        /// </returns>
        static Exception^ Get(const apache::geode::client::Exception& nativeEx);

        /// <summary>
        /// Get the stack trace for the given native exception.
        /// </summary>
        /// <param name="nativeEx">The native Geode exception object</param>
        /// <returns>The stack trace of the native exception.</returns>
        inline static String^ GetStackTrace(
          const apache::geode::client::Exception& nativeEx )
        {
          char nativeExStack[2048] = { '\0' };
#ifndef _WIN64
          nativeEx.getStackTrace(nativeExStack, 2047);
#endif
          return ManagedString::Get(nativeExStack);
        }

        /// <summary>
        /// Gets the C++ native exception object for a given managed exception.
        /// </summary>
        /// <remarks>
        /// This method is to handle conversion of managed exceptions to
        /// C++ exception for those thrown by managed callbacks.
        /// For non-Geode .NET exceptions we wrap it inside the generic
        /// <c>GeodeException</c> with a special prefix in message.
        /// While converting the exception back from C++ to .NET if the
        /// prefix is found in the message, then it tries to construct
        /// the original exception by reflection on the name of exception
        /// contained in the message. Note that in this process the
        /// original stacktrace is appended to the message of the exception.
        /// </remarks>
        inline static apache::geode::client::ExceptionPtr GetNative(Exception^ ex)
        {
          if (ex != nullptr) {
            GeodeException^ gfEx = dynamic_cast<GeodeException^>(ex);
            if (gfEx != nullptr) {
              return gfEx->GetNative();
            }
            else {
              apache::geode::client::ExceptionPtr cause;
              if (ex->InnerException != nullptr) {
                cause = GeodeException::GetNative(ex->InnerException);
              }
              ManagedString mg_exStr(MgSysExPrefix + ex->ToString());
              return apache::geode::client::ExceptionPtr(new apache::geode::client::Exception(
                  mg_exStr.CharPtr, NULL, false, cause));
            }
          }
          return NULLPTR;
        }

        /// <summary>
        /// Gets the C++ native exception object for this managed
        /// <c>GeodeException</c>.
        /// </summary>
        virtual apache::geode::client::ExceptionPtr GetNative()
        {
          String^ msg = this->Message + ": " + this->StackTrace;
          ManagedString mg_msg(msg);
          apache::geode::client::ExceptionPtr cause;
          if (this->InnerException != nullptr) {
            cause = GeodeException::GetNative(this->InnerException);
          }
          return apache::geode::client::ExceptionPtr(new apache::geode::client::Exception(mg_msg.CharPtr,
              NULL, false, cause));
        }

        /// <summary>
        /// Throws the C++ native exception object for the given .NET exception.
        /// </summary>
        /// <remarks>
        /// This method is to handle conversion of managed exceptions to
        /// C++ exception for those thrown by managed callbacks.
        /// For non-Geode .NET exceptions we wrap it inside the generic
        /// <c>GeodeException</c> with a special prefix in message.
        /// While converting the exception back from C++ to .NET if the
        /// prefix is found in the message, then it tries to construct
        /// the original exception by reflection on the name of exception
        /// contained in the message. Note that in this process the
        /// original stacktrace is appended to the message of the exception.
        /// </remarks>
        inline static void ThrowNative(Exception^ ex)
        {
          if (ex != nullptr) {
            apache::geode::client::ExceptionPtr cause;
            if (ex->InnerException != nullptr) {
              cause = GeodeException::GetNative(ex->InnerException);
            }
            ManagedString mg_exStr(MgSysExPrefix + ex->ToString());
            throw apache::geode::client::Exception(mg_exStr.CharPtr, NULL, false, cause);
          }
        }

        /// <summary>
        /// Throws the C++ native exception object for this managed
        /// <c>GeodeException</c>.
        /// </summary>
        inline void ThrowNative()
        {
          GetNative()->raise();
        }


      public:

        /// <summary>
        /// Default constructor.
        /// </summary>
        inline GeodeException( )
          : Exception( ) { }

        /// <summary>
        /// Constructor to create an exception object with the given message.
        /// </summary>
        /// <param name="message">The exception message.</param>
        inline GeodeException( String^ message )
          : Exception( message ) { }

        /// <summary>
        /// Constructor to create an exception object with the given message
        /// and with the given inner exception.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="innerException">The inner exception object.</param>
        inline GeodeException( String^ message, System::Exception^ innerException )
          : Exception( message, innerException ) { }

      protected:

        /// <summary>
        /// Initializes a new instance of the <c>GeodeException</c> class with
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
        inline GeodeException( SerializationInfo^ info, StreamingContext context )
          : Exception( info, context ) { }
      };

/// Handle geode exceptions from native layer and convert to managed
/// exceptions.
#define _GF_MG_EXCEPTION_TRY2        \
      try {
#define _GF_MG_EXCEPTION_CATCH_ALL2  \
      } \
      catch (const apache::geode::client::Exception& ex) { \
      throw Apache::Geode::Client::GeodeException::Get(ex); \
      } \
      catch (System::AccessViolationException^ ex) { \
        throw ex; \
      }


/// Creates a class <c>x</c> named for each exception <c>y</c>.
#define _GF_MG_EXCEPTION_DEF4(x,y) \
      [Serializable] \
      public ref class x: public GeodeException \
      { \
      public: \
      \
        /** <summary>Default constructor</summary> */ \
        x( ) \
          : GeodeException( ) { } \
        \
        /** <summary>
         *  Constructor to create an exception object with the given message.
         *  </summary>
         *  <param name="message">The exception message.</param>
         */ \
        x( String^ message ) \
          : GeodeException( message ) { } \
        \
        /** <summary>
         *  Constructor to create an exception object with the given message
         *  and with the given inner exception.
         *  </summary>
         *  <param name="message">The exception message.</param>
         *  <param name="innerException">The inner exception object.</param>
         */ \
        x( String^ message, System::Exception^ innerException ) \
          : GeodeException( message, innerException ) { } \
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
          : GeodeException( info, context ) { } \
      \
      internal: \
        x(const apache::geode::client::y& nativeEx) \
          : GeodeException(ManagedString::Get(nativeEx.getMessage()), \
              gcnew GeodeException(GeodeException::GetStackTrace( \
                nativeEx))) { } \
        \
        x(const apache::geode::client::y& nativeEx, Exception^ innerException) \
          : GeodeException(ManagedString::Get(nativeEx.getMessage()), \
              innerException) { } \
        \
        static GeodeException^ Create(const apache::geode::client::Exception& ex, \
            Exception^ innerException) \
        { \
          const apache::geode::client::y* nativeEx = dynamic_cast<const apache::geode::client::y*>( &ex ); \
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
        virtual apache::geode::client::ExceptionPtr GetNative() override \
        { \
          String^ msg = this->Message + ": " + this->StackTrace; \
          ManagedString mg_msg(msg); \
          apache::geode::client::ExceptionPtr cause; \
          if (this->InnerException != nullptr) { \
            cause = GeodeException::GetNative(this->InnerException); \
          } \
          return apache::geode::client::ExceptionPtr(new apache::geode::client::y(mg_msg.CharPtr, \
              NULL, false, cause)); \
        } \
      }

/// Creates a class named for each exception <c>x</c>.
#define _GF_MG_EXCEPTION_DEF3(x) _GF_MG_EXCEPTION_DEF4(x,x)


      // For all the native Geode C++ exceptions, a corresponding definition
      // should be added below *AND* it should also be added to the static array
      // in ExceptionTypesMN.cpp using _GF_MG_EXCEPTION_ADD3( x )

      /// <summary>
      /// A geode assertion exception.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( AssertionException );

      /// <summary>
      /// Thrown when an argument to a method is illegal.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( IllegalArgumentException );

      /// <summary>
      /// Thrown when the state of cache is manipulated to be illegal.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( IllegalStateException );

      /// <summary>
      /// Thrown when an attempt is made to create an existing cache.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CacheExistsException );

      /// <summary>
      /// Thrown when the cache xml is incorrect.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CacheXmlException );

      /// <summary>
      /// Thrown when a timout occurs.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( TimeoutException );

      /// <summary>
      /// Thrown when the cache writer aborts the operation.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CacheWriterException );

      /// <summary>
      /// Thrown when the cache listener throws an exception.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CacheListenerException );

      /// <summary>
      /// Thrown when an attempt is made to create an existing region.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( RegionExistsException );

      /// <summary>
      /// Thrown when an operation is attempted on a closed cache.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CacheClosedException );

      /// <summary>
      /// Thrown when lease of cache proxy has expired.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( LeaseExpiredException );

      /// <summary>
      /// Thrown when the cache loader aborts the operation.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CacheLoaderException );

      /// <summary>
      /// Thrown when an operation is attempted on a destroyed region.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( RegionDestroyedException );

      /// <summary>
      /// Thrown when an operation is attempted on a destroyed entry.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( EntryDestroyedException );

      /// <summary>
      /// Thrown when the connecting target is not running.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( NoSystemException );

      /// <summary>
      /// Thrown when an attempt is made to connect to
      /// DistributedSystem second time.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( AlreadyConnectedException );

      /// <summary>
      /// Thrown when a non-existing file is accessed.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( FileNotFoundException );

      /// <summary>
      /// Thrown when an operation is interrupted.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( InterruptedException );

      /// <summary>
      /// Thrown when an operation unsupported by the
      /// current configuration is attempted.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( UnsupportedOperationException );

      /// <summary>
      /// Thrown when statistics are invoked for a region where
      /// they are disabled.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( StatisticsDisabledException );

      /// <summary>
      /// Thrown when a concurrent operation fails.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( ConcurrentModificationException );

      /// <summary>
      /// An unknown exception occurred.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( UnknownException );

      /// <summary>
      /// Thrown when a cast operation fails.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( ClassCastException );

      /// <summary>
      /// Thrown when an operation is attempted on a non-existent entry.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( EntryNotFoundException );

      /// <summary>
      /// Thrown when there is an input/output error.
      /// </summary>
      _GF_MG_EXCEPTION_DEF4( GeodeIOException, GeodeIOException );

      /// <summary>
      /// Thrown when geode configuration file is incorrect.
      /// </summary>
      _GF_MG_EXCEPTION_DEF4( GeodeConfigException, GeodeConfigException );

      /// <summary>
      /// Thrown when a null argument is provided to a method
      /// where it is expected to be non-null.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( NullPointerException );

      /// <summary>
      /// Thrown when attempt is made to create an existing entry.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( EntryExistsException );

      /// <summary>
      /// Thrown when an operation is attempted before connecting
      /// to the distributed system.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( NotConnectedException );

      /// <summary>
      /// Thrown when there is an error in the cache proxy.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CacheProxyException );

      /// <summary>
      /// Thrown when the system cannot allocate any more memory.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( OutOfMemoryException );

      /// <summary>
      /// Thrown when an attempt is made to release a lock not
      /// owned by the thread.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( NotOwnerException );

      /// <summary>
      /// Thrown when a region is created in an incorrect scope.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( WrongRegionScopeException );

      /// <summary>
      /// Thrown when the internal buffer size is exceeded.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( BufferSizeExceededException );

      /// <summary>
      /// Thrown when a region creation operation fails.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( RegionCreationFailedException );

      /// <summary>
      /// Thrown when there is a fatal internal exception in Geode.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( FatalInternalException );

      /// <summary>
      /// Thrown by the persistence manager when a write
      /// fails due to disk failure.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( DiskFailureException );

      /// <summary>
      /// Thrown by the persistence manager when the data
      /// to be read from disk is corrupt.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( DiskCorruptException );

      /// <summary>
      /// Thrown when persistence manager fails to initialize.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( InitFailedException );

      /// <summary>
      /// Thrown when persistence manager fails to close properly.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( ShutdownFailedException );

      /// <summary>
      /// Thrown when an exception occurs on the cache server.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CacheServerException );

      /// <summary>
      /// Thrown when bound of array/vector etc. is exceeded.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( OutOfRangeException );

      /// <summary>
      /// Thrown when query exception occurs at the server.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( QueryException );

      /// <summary>
      /// Thrown when an unknown message is received from the server.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( MessageException );

      /// <summary>
      /// Thrown when a client operation is not authorized on the server.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( NotAuthorizedException );

      /// <summary>
      /// Thrown when authentication to the server fails.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( AuthenticationFailedException );

      /// <summary>
      /// Thrown when credentials are not provided to a server which expects them.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( AuthenticationRequiredException );

      /// <summary>
      /// Thrown when a duplicate durable client id is provided to the server.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( DuplicateDurableClientException );

      /// <summary>
      /// Thrown when a client is unable to contact any locators.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( NoAvailableLocatorsException );

      /// <summary>
      /// Thrown when all connections in a pool are in use..
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( AllConnectionsInUseException );

      /// <summary>
      /// Thrown when cq is invalid
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CqInvalidException );

      /// <summary>
      /// Thrown when function execution failed
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( FunctionExecutionException );

      /// <summary>
      /// Thrown during continuous query execution time.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CqException );

      /// <summary>
      /// Thrown if the Cq on which the operaion performed is closed
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CqClosedException );

      /// <summary>
      /// Thrown if the Cq Query failed
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CqQueryException );

      /// <summary>
      /// Thrown if a Cq by this name already exists on this client
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CqExistsException );

      _GF_MG_EXCEPTION_DEF3( InvalidDeltaException );

      /// <summary>
      /// Thrown if a Key is not present in the region.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( KeyNotFoundException );

      /// <summary>
      /// Thrown if commit fails.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( CommitConflictException );

	        /// <summary>
      /// Thrown if transaction delegate went down.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( TransactionDataNodeHasDepartedException );

	        /// <summary>
      /// Thrown if commit rebalance happens during a transaction.
      /// </summary>
      _GF_MG_EXCEPTION_DEF3( TransactionDataRebalancedException );
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

