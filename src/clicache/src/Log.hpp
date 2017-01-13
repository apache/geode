/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/Log.hpp>


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {

      /// <summary>
      /// Logging levels.
      /// </summary>
      public enum class LogLevel
      {
        /// <summary>
        /// No log.
        /// </summary>
        Null = 0,

        /// <summary>
        /// Indicates serious failure.
        /// </summary>
        Error,
        /// <summary>
        /// Indicates potential problem.
        /// </summary>
        Warning,
        /// <summary>
        /// For informational purpose.
        /// </summary>
        Info,

        /// <summary>
        /// The default logging level.
        /// </summary>
        Default,

        /// <summary>
        /// For Static configuration messages.
        /// </summary>
        Config,

        /// <summary>
        /// For tracing information.
        /// </summary>
        Fine,
        /// <summary>
        /// For moderately detailed tracing information.
        /// </summary>
        Finer,
        /// <summary>
        /// For very detailed tracing information.
        /// </summary>
        Finest,

        /// <summary>
        /// For highly detailed tracing information.
        /// </summary>
        Debug,

        /// <summary>
        /// All the log messages.
        /// </summary>
        All,
      };


      /// <summary>
      /// Defines methods available to clients that want to write a log message
      /// to their GemFire system's shared log file.
      /// </summary>
      /// <remarks>
      /// Any attempt to use an instance after its connection is disconnected
      /// will throw a <c>NotConnectedException</c>.
      /// <para>
      /// For any logged message the log file will contain:
      /// <ul>
      /// <li> The message's log level.</li>
      /// <li> The time the message was logged.</li>
      /// <li> The ID of the connection and thread that logged the message.</li>
      /// <li> The message itself, perhaps with
      /// an exception including the exception's stack trace.</li>
      /// </ul>
      /// </para><para>
      /// A message always has a level.
      /// Logging levels are ordered. Enabling logging at a given level also
      /// enables logging at higher levels. The higher the level the more
      /// important and urgent the message.
      /// </para><para>
      /// The levels, in descending order of severity, are:
      /// <ul>
      ///
      /// <li> <c>Error</c> (highest severity) is a message level
      /// indicating a serious failure.  In general <c>error</c>
      /// messages should describe events that are of considerable
      /// importance and which will prevent normal program execution. They
      /// should be reasonably intelligible to end users and to system
      /// administrators.</li>
      ///
      /// <li> <c>Warning</c> is a message level indicating a
      /// potential problem.  In general <c>warning</c> messages
      /// should describe events that will be of interest to end users or
      /// system managers, or which indicate potential problems.</li>
      ///
      /// <li> <c>Info</c> is a message level for informational
      /// messages.  Typically <c>info</c> messages should be
      /// reasonably significant and should make sense to end users and
      /// system administrators.</li>
      ///
      /// <li> <c>Config</c> is a message level for static
      /// configuration messages.  <c>config</c> messages are intended
      /// to provide a variety of static configuration information, to
      /// assist in debugging problems that may be associated with
      /// particular configurations.</li>
      ///
      /// <li> <c>Fine</c> is a message level providing tracing
      /// information.  In general the <c>fine</c> level should be
      /// used for information that will be broadly interesting to
      /// developers. This level is for the lowest volume, and most
      /// important, tracing messages.</li>
      ///
      /// <li> <c>Finer</c> indicates a moderately detailed tracing
      /// message.  This is an intermediate level between <c>fine</c>
      /// and <c>finest</c>.</li>
      ///
      /// <li> <c>Finest</c> indicates a very detailed tracing
      /// message.  Logging calls for entering, returning, or throwing an
      /// exception are traced at the <c>finest</c> level.</li>
      ///
      /// <li> <c>Debug</c> (lowest severity) indicates a highly
      /// detailed tracing message.  In general the <c>debug</c> level
      /// should be used for the most voluminous detailed tracing messages.</li>
      /// </ul>
      ///
      /// </para>
      /// </remarks>
      public ref class Log STATICCLASS
      {
      public:



        /// <summary>
        /// Initializes the logging facility with the given level and filename.
        /// </summary>
        /// <param name="level">the logging level</param>
        /// <param name="logFileName">the log file name</param>
        static void Init( LogLevel level, String^ logFileName );

        /// <summary>
        /// Initializes logging facility with given level, filename, and file size limit.
        /// </summary>
        /// <param name="level">the logging level</param>
        /// <param name="logFileName">the log file name</param>
        /// <param name="logFileLimit">maximum allowable size of the log file, in bytes, 
        ///        or 0 for the default (1 Gbyte)</param>
        static void Init( LogLevel level, String^ logFileName, int32_t logFileLimit );

        /// <summary>
        /// Closes logging facility (until next init).
        /// </summary>
        static void Close( );

        /// <summary>
        /// Returns the current log level.
        /// </summary>
        static LogLevel Level( );

        /// <summary>
        /// Sets the current log level.
        /// </summary>
        static void SetLevel( LogLevel level );

        /// <summary>
        /// Returns the name of the current log file.
        /// NOTE: This function is for debugging only, as it is not completely
        /// thread-safe!
        /// </summary>
        static String^ LogFileName( );

        /// <summary>
        /// True if log messages at the given level are enabled.
        /// </summary>
        static bool Enabled( LogLevel level );

        /// <summary>
        /// Logs a message at the given level.
        /// </summary>
        static void Write( LogLevel level, String^ msg );

        /// <summary>
        /// Logs both a message and a thrown exception.
        /// </summary>
        static void LogThrow( LogLevel level, String^ msg, System::Exception^ ex );

        /// <summary>
        /// Logs both a message and a caught exception.
        /// </summary>
        static void LogCatch( LogLevel level, String^ msg, System::Exception^ ex );

        // Convenience functions with variable number of arguments
        // as in String.Format

        /// <summary>
        /// Error level logging with variable number of arguments using
        /// format as in <c>System.String.Format</c>.
        /// </summary>
        inline static void Error( String^ format, ... array<Object^>^ args )
        {
          if(staticLogLevel >= LogLevel::Error)
            Log::Write( LogLevel::Error, String::Format(
              System::Globalization::CultureInfo::CurrentCulture, format, args ) );
        }

        /// <summary>
        /// Warning level logging with variable number of arguments using
        /// format as in <c>System.String.Format</c>.
        /// </summary>
        inline static void Warning( String^ format, ... array<Object^>^ args )
        {
          if(staticLogLevel >= LogLevel::Warning)
            Log::Write( LogLevel::Warning, String::Format(
              System::Globalization::CultureInfo::CurrentCulture, format, args ) );
        }

        /// <summary>
        /// Info level logging with variable number of arguments using
        /// format as in <c>System.String.Format</c>.
        /// </summary>
        inline static void Info( String^ format, ... array<Object^>^ args )
        {
          if(staticLogLevel >= LogLevel::Info)
            Log::Write( LogLevel::Info, String::Format(
              System::Globalization::CultureInfo::CurrentCulture, format, args ) );
        }

        /// <summary>
        /// Config level logging with variable number of arguments using
        /// format as in <c>System.String.Format</c>.
        /// </summary>
        inline static void Config( String^ format, ... array<Object^>^ args )
        {
          if(staticLogLevel >= LogLevel::Config)
            Log::Write( LogLevel::Config, String::Format(
              System::Globalization::CultureInfo::CurrentCulture, format, args ) );
        }

        /// <summary>
        /// Fine level logging with variable number of arguments using
        /// format as in <c>System.String.Format</c>.
        /// </summary>
        inline static void Fine( String^ format, ... array<Object^>^ args )
        {
          if(staticLogLevel >= LogLevel::Fine)
            Log::Write( LogLevel::Fine, String::Format(
              System::Globalization::CultureInfo::CurrentCulture, format, args ) );
        }

        /// <summary>
        /// Finer level logging with variable number of arguments using
        /// format as in <c>System.String.Format</c>.
        /// </summary>
        inline static void Finer( String^ format, ... array<Object^>^ args )
        {
          if(staticLogLevel >= LogLevel::Finer)
            Log::Write( LogLevel::Finer, String::Format(
              System::Globalization::CultureInfo::CurrentCulture, format, args ) );
        }

        /// <summary>
        /// Finest level logging with variable number of arguments using
        /// format as in <c>System.String.Format</c>.
        /// </summary>
        inline static void Finest( String^ format, ... array<Object^>^ args )
        {
          if(staticLogLevel >= LogLevel::Finest)
            Log::Write( LogLevel::Finest, String::Format(
              System::Globalization::CultureInfo::CurrentCulture, format, args ) );
        }

        /// <summary>
        /// Debug level logging with variable number of arguments using
        /// format as in <c>System.String.Format</c>.
        /// </summary>
        inline static void Debug( String^ format, ... array<Object^>^ args )
        {
          if(staticLogLevel >= LogLevel::Debug)
          Log::Write( LogLevel::Debug, String::Format(
            System::Globalization::CultureInfo::CurrentCulture, format, args ) );
        }
      internal:

        static void SetLogLevel( LogLevel level)
        {
          staticLogLevel = level;
        }

       private:
         static LogLevel staticLogLevel =  LogLevel::Null;
      };
      } // end namespace Generic
    }
  }
}
