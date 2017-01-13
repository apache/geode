/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "Log.hpp"
#include "impl/ManagedString.hpp"
#include "impl/SafeConvert.hpp"
#include "ExceptionTypes.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
      void Log::Init( LogLevel level, String^ logFileName )
      {
        _GF_MG_EXCEPTION_TRY2

          ManagedString mg_lfname( logFileName );
          gemfire::Log::init( static_cast<gemfire::Log::LogLevel>( level ),
            mg_lfname.CharPtr );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      void Log::Init( LogLevel level, String^ logFileName, int32_t logFileLimit )
      {
        _GF_MG_EXCEPTION_TRY2

          ManagedString mg_lfname( logFileName );
          gemfire::Log::init( static_cast<gemfire::Log::LogLevel>( level ),
            mg_lfname.CharPtr, logFileLimit );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      void Log::Close( )
      {
        gemfire::Log::close( );
      }

      LogLevel Log::Level( )
      {
        return static_cast<LogLevel>( gemfire::Log::logLevel( ) );
      }

      void Log::SetLevel( LogLevel level )
      {
        gemfire::Log::setLogLevel(
          static_cast<gemfire::Log::LogLevel>( level ) );
      }

      String^ Log::LogFileName( )
      {
        _GF_MG_EXCEPTION_TRY2

          return ManagedString::Get( gemfire::Log::logFileName( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      bool Log::Enabled( LogLevel level )
      {
        return gemfire::Log::enabled(
          static_cast<gemfire::Log::LogLevel>( level ) );
      }

      void Log::Write( LogLevel level, String^ msg )
      {
        _GF_MG_EXCEPTION_TRY2

          ManagedString mg_msg( msg );
          gemfire::Log::log( static_cast<gemfire::Log::LogLevel>( level ),
            mg_msg.CharPtr );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      void Log::LogThrow( LogLevel level, String^ msg, System::Exception^ ex )
      {
        if ( ex != nullptr )
        {
          String^ logMsg = String::Format(
            System::Globalization::CultureInfo::CurrentCulture,
            "GemFire exception {0} thrown: {1}{2}{3}", ex->GetType( ),
            ex->Message, Environment::NewLine, msg );
          Log::Write( level, logMsg );
        }
      }

      void Log::LogCatch( LogLevel level, String^ msg, System::Exception^ ex )
      {
        if ( ex != nullptr )
        {
          String^ logMsg = String::Format(
            System::Globalization::CultureInfo::CurrentCulture,
            "GemFire exception {0} caught: {1}{2}{3}", ex->GetType( ),
            ex->Message, Environment::NewLine, msg );
          Log::Write( level, logMsg );
        }
      }
      } // end namespace generic
    }
  }
}
