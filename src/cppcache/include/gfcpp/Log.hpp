#ifndef _GEMFIRE_LOG_HPP_
#define _GEMFIRE_LOG_HPP_

/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
 * The interface of the Log class
 *
 *
 */

#include "gfcpp_globals.hpp"
#include <stdio.h>
#include <stdarg.h>

/******************************************************************************/

#ifndef GEMFIRE_HIGHEST_LOG_LEVEL
#define GEMFIRE_HIGHEST_LOG_LEVEL All
#endif

#ifndef GEMFIRE_MAX_LOG_FILE_LIMIT
#define GEMFIRE_MAX_LOG_FILE_LIMIT (1024 * 1024 * 1024)
#endif

#ifndef GEMFIRE_MAX_LOG_DISK_LIMIT
#define GEMFIRE_MAX_LOG_DISK_LIMIT (1024ll * 1024ll * 1024ll * 1024ll)
#endif

#define _GF_MSG_LIMIT 8192

/******************************************************************************/

/** @file
*/

namespace gemfire {

class Exception;

/******************************************************************************/
/******************************************************************************/

/* Logs the message if the given level is less than or equal to the current
 * logging level. */
#define GF_LOG(level, expr)               \
  if (level > gemfire::Log::logLevel()) { \
  } else                                  \
    gemfire::Log::log(level, expr)

/** Defines methods available to clients that want to write a log message
  * to their GemFire system's shared log file.
  * <p>
  * This class must be initialized prior to its use:
  * @ref Log::init
  * <p>
  * For any logged message the log file will contain:
  * <ul>
  * <li> The message's level.
  * <li> The time the message was logged.
  * <li> The id of the connection and thread that logged the message.
  * <li> The message itself which can be a const char* (perhaps with
  * an exception including the exception's stack trace.
  * </ul>
  * <p>
  * A message always has a level.
  * Logging levels are ordered. Enabling logging at a given level also
  * enables logging at higher levels. The higher the level the more
  * important and urgent the message.
  * <p>
  * The levels, in descending order of severity, are:
  * <ul>
  *
  * <li> <code>error</code> (highest severity) is a message level
  * indicating a serious failure.  In general <code>error</code>
  * messages should describe events that are of considerable
  * importance and which will prevent normal program execution. They
  * should be reasonably intelligible to end users and to system
  * administrators.
  *
  * <li> <code>warning</code> is a message level indicating a
  * potential problem.  In general <code>warning</code> messages
  * should describe events that will be of interest to end users or
  * system managers, or which indicate potential problems.
  *
  * <li> <code>info</code> is a message level for informational
  * messages.  Typically <code>info</code> messages should be
  * reasonably significant and should make sense to end users and
  * system administrators.
  *
  * <li> <code>config</code> is a message level for static
  * configuration messages.  <code>config</code> messages are intended
  * to provide a variety of static configuration information, to
  * assist in debugging problems that may be associated with
  * particular configurations.
  *
  * <li> <code>fine</code> is a message level providing tracing
  * information.  In general the <code>fine</code> level should be
  * used for information that will be broadly interesting to
  * developers. This level is for the lowest volume, and most
  * important, tracing messages.
  *
  * <li> <code>finer</code> indicates a moderately detailed tracing
  * message.  This is an intermediate level between <code>fine</code>
  * and <code>finest</code>.
  *
  * <li> <code>finest</code> indicates a very detailed tracing
  * message.  Logging calls for entering, returning, or throwing an
  * exception are traced at the <code>finest</code> level.
  *
  * <li> <code>debug</code> (lowest severity) indicates a highly
  * detailed tracing message.  In general the <code>debug</code> level
  * should be used for the most voluminous detailed tracing messages.
  * </ul>
  *
  * <p>
  * For each level methods exist that will request a message, at that
  * level, to be logged. These methods are all named after their level.
  * <p>
  * For each level a method exists that indicates if messages at that
  * level will currently be logged. The names of these methods are of
  * the form: <em>level</em><code>Enabled</code>.
  *
  *
  */

class CPPCACHE_EXPORT Log {
 public:
  /******/

  enum LogLevel {

    // NOTE: if you change this enum declaration at all, be sure to
    // change levelToChars and charsToLevel functions!

    None,

    Error,
    Warning,
    Info,

    Default,

    Config,

    Fine,
    Finer,
    Finest,

    Debug,

    All

  };

  /******/

  /**
   * Returns the current log level.
   */
  static LogLevel logLevel() { return s_logLevel; }

  /**
   * Set the current log level.
   */
  static void setLogLevel(LogLevel level) { s_logLevel = level; }

  /**
   * @return the name of the current log file.
   * NOTE: This function is for debugging only, as it is not completely
   * thread-safe!
   */
  static const char* logFileName();

  /**
   * Initializes logging facility with given level and filenames.
   * This method is called automatically within @ref DistributedSystem::connect
   * with the log-file, log-level, and log-file-size system properties used as
   * arguments
   */
  static void init
      // 0 => use maximum value (currently 1G)
      (LogLevel level, const char* logFileName, int32 logFileLimit = 0,
       int64 logDiskSpaceLimit = 0);

  /**
   * closes logging facility (until next init).
   */
  static void close();

  /**
   * returns character string for given log level. The string will be
   * identical to the enum declaration above, except it will be all
   * lower case. Out of range values will throw
   * IllegalArgumentException.
   */
  static const char* levelToChars(Log::LogLevel level);

  /**
   * returns log level specified by "chars", or throws
   * IllegalArgumentException.  Allowed values are identical to the
   * enum declaration above for LogLevel, but with character case ignored.
   */
  static LogLevel charsToLevel(const char* chars);

  /**
   * Fills the provided buffer with formatted log-line given the level
   * and returns the buffer. This assumes that the buffer has large
   * enough space left to hold the formatted log-line (around 70 chars).
   *
   * This is provided so that applications wishing to use the same format
   * as GemFire log-lines can do so easily. A log-line starts with the prefix
   * given below which is filled in by this method:
   * [<level> <date> <time> <timezone> <host>:<process ID> <thread ID>]
   *
   * This method is not thread-safe for the first invocation.
   * When invoking from outside either <init> should have been invoked,
   * or at least the first invocation should be single-threaded.
   */
  static char* formatLogLine(char* buf, LogLevel level);

  /******/

  /**
   * Returns whether log messages at given level are enabled.
   */
  static bool enabled(LogLevel level) {
    return (((s_doingDebug && level == Debug) ||
             GEMFIRE_HIGHEST_LOG_LEVEL >= level) &&
            s_logLevel >= level);
  }

  /**
   * Logs a message at given level.
   */
  static void log(LogLevel level, const char* msg) {
    if (enabled(level)) put(level, msg);
  }

  /**
   * Logs both a message and thrown exception.
   */
  static void logThrow(LogLevel level, const char* msg, const Exception& ex) {
    if (enabled(level)) putThrow(level, msg, ex);
  }

  /**
   * Logs both a message and caught exception.
   */
  static void logCatch(LogLevel level, const char* msg, const Exception& ex) {
    if (enabled(level)) putCatch(level, msg, ex);
  }

  /******/

  /**
   * Returns whether "error" log messages are enabled.
   */
  static bool errorEnabled() {
    return GEMFIRE_HIGHEST_LOG_LEVEL >= Error && s_logLevel >= Error;
  }

  /**
   * Logs a message.
   * The message level is "error".
   */
  static void error(const char* msg) {
    if (errorEnabled()) put(Error, msg);
  }

  /**
   * Logs both a message and thrown exception.
   * The message level is "error".
   */
  static void errorThrow(const char* msg, const Exception& ex) {
    if (errorEnabled()) putThrow(Error, msg, ex);
  }

  /**
   * Writes both a message and caught exception.
   * The message level is "error".
   */
  static void errorCatch(const char* msg, const Exception& ex) {
    if (errorEnabled()) putCatch(Error, msg, ex);
  }

  /******/

  /**
   * Returns whether "warning" log messages are enabled.
   */
  static bool warningEnabled() {
    return GEMFIRE_HIGHEST_LOG_LEVEL >= Warning && s_logLevel >= Warning;
  }

  /**
   * Logs a message.
   * The message level is "warning".
   */
  static void warning(const char* msg) {
    if (warningEnabled()) put(Warning, msg);
  }

  /**
   * Logs both a message and thrown exception.
   * The message level is "warning".
   */
  static void warningThrow(const char* msg, const Exception& ex) {
    if (warningEnabled()) putThrow(Warning, msg, ex);
  }

  /**
   * Writes both a message and caught exception.
   * The message level is "warning".
   */
  static void warningCatch(const char* msg, const Exception& ex) {
    if (warningEnabled()) putCatch(Warning, msg, ex);
  }

  /******/

  /**
   * Returns whether "info" log messages are enabled.
   */
  static bool infoEnabled() {
    return GEMFIRE_HIGHEST_LOG_LEVEL >= Info && s_logLevel >= Info;
  }

  /**
   * Logs a message.
   * The message level is "info".
   */
  static void info(const char* msg) {
    if (infoEnabled()) put(Info, msg);
  }

  /**
   * Logs both a message and thrown exception.
   * The message level is "info".
   */
  static void infoThrow(const char* msg, const Exception& ex) {
    if (infoEnabled()) putThrow(Info, msg, ex);
  }

  /**
   * Writes both a message and caught exception.
   * The message level is "info".
   */
  static void infoCatch(const char* msg, const Exception& ex) {
    if (infoEnabled()) putCatch(Info, msg, ex);
  }

  /******/

  /**
   * Returns whether "config" log messages are enabled.
   */
  static bool configEnabled() {
    return GEMFIRE_HIGHEST_LOG_LEVEL >= Config && s_logLevel >= Config;
  }

  /**
   * Logs a message.
   * The message level is "config".
   */
  static void config(const char* msg) {
    if (configEnabled()) put(Config, msg);
  }

  /**
   * Logs both a message and thrown exception.
   * The message level is "config".
   */
  static void configThrow(const char* msg, const Exception& ex) {
    if (configEnabled()) putThrow(Config, msg, ex);
  }

  /**
   * Writes both a message and caught exception.
   * The message level is "config".
   */
  static void configCatch(const char* msg, const Exception& ex) {
    if (configEnabled()) putCatch(Config, msg, ex);
  }

  /******/

  /**
   * Returns whether "fine" log messages are enabled.
   */
  static bool fineEnabled() {
    return GEMFIRE_HIGHEST_LOG_LEVEL >= Fine && s_logLevel >= Fine;
  }

  /**
   * Logs a message.
   * The message level is "fine".
   */
  static void fine(const char* msg) {
    if (fineEnabled()) put(Fine, msg);
  }

  /**
   * Logs both a message and thrown exception.
   * The message level is "fine".
   */
  static void fineThrow(const char* msg, const Exception& ex) {
    if (fineEnabled()) putThrow(Fine, msg, ex);
  }

  /**
   * Writes both a message and caught exception.
   * The message level is "fine".
   */
  static void fineCatch(const char* msg, const Exception& ex) {
    if (fineEnabled()) putCatch(Fine, msg, ex);
  }

  /******/

  /**
   * Returns whether "finer" log messages are enabled.
   */
  static bool finerEnabled() {
    return GEMFIRE_HIGHEST_LOG_LEVEL >= Finer && s_logLevel >= Finer;
  }

  /**
   * Logs a message.
   * The message level is "finer".
   */
  static void finer(const char* msg) {
    if (finerEnabled()) put(Finer, msg);
  }

  /**
   * Logs both a message and thrown exception.
   * The message level is "finer".
   */
  static void finerThrow(const char* msg, const Exception& ex) {
    if (finerEnabled()) putThrow(Finer, msg, ex);
  }

  /**
   * Writes both a message and caught exception.
   * The message level is "finer".
   */
  static void finerCatch(const char* msg, const Exception& ex) {
    if (finerEnabled()) putCatch(Finer, msg, ex);
  }

  /******/

  /**
   * Returns whether "finest" log messages are enabled.
   */
  static bool finestEnabled() {
    return GEMFIRE_HIGHEST_LOG_LEVEL >= Finest && s_logLevel >= Finest;
  }

  /**
   * Logs a message.
   * The message level is "finest".
   */
  static void finest(const char* msg) {
    if (finestEnabled()) put(Finest, msg);
  }

  /**
   * Logs both a message and thrown exception.
   * The message level is "finest".
   */
  static void finestThrow(const char* msg, const Exception& ex) {
    if (finestEnabled()) putThrow(Finest, msg, ex);
  }

  /**
   * Writes both a message and caught exception.
   * The message level is "finest".
   */
  static void finestCatch(const char* msg, const Exception& ex) {
    if (finestEnabled()) putCatch(Finest, msg, ex);
  }

  /******/

  /**
   * Returns whether "debug" log messages are enabled.
   */
  static bool debugEnabled() {
    return (s_doingDebug || GEMFIRE_HIGHEST_LOG_LEVEL >= Debug) &&
           s_logLevel >= Debug;
  }

  /**
   * Logs a message.
   * The message level is "debug".
   */
  static void debug(const char* msg) {
    if (debugEnabled()) put(Debug, msg);
  }

  /**
   * Logs both a message and thrown exception.
   * The message level is "debug".
   */
  static void debugThrow(const char* msg, const Exception& ex) {
    if (debugEnabled()) putThrow(Debug, msg, ex);
  }

  /**
   * Writes both a message and caught exception.
   * The message level is "debug".
   */
  static void debugCatch(const char* msg, const Exception& ex) {
    if (debugEnabled()) putCatch(Debug, msg, ex);
  }

  /******/

  static void enterFn(LogLevel level, const char* functionName);

  static void exitFn(LogLevel level, const char* functionName);

  /******/

 private:
  static LogLevel s_logLevel;

/******/

#ifdef DEBUG
  enum { s_doingDebug = 1 };
#else
  enum { s_doingDebug = 0 };
#endif

  /******/

  static void writeBanner();

  /******/
 public:
  static void put(LogLevel level, const char* msg);

  static void putThrow(LogLevel level, const char* msg, const Exception& ex);

  static void putCatch(LogLevel level, const char* msg, const Exception& ex);
};

/******************************************************************************/
/******************************************************************************/

class LogFn {
  const char* m_functionName;
  Log::LogLevel m_level;

 public:
  LogFn(const char* functionName, Log::LogLevel level = Log::Finest)
      : m_functionName(functionName), m_level(level) {
    if (Log::enabled(m_level)) Log::enterFn(m_level, m_functionName);
  }

  ~LogFn() {
    if (Log::enabled(m_level)) Log::exitFn(m_level, m_functionName);
  }

 private:
  LogFn(const LogFn& rhs);           // never defined
  void operator=(const LogFn& rhs);  // never defined
};

/******************************************************************************/
/******************************************************************************/

/**
 * These functions are added to facilitate logging in printf format.
 */

class CPPCACHE_EXPORT LogVarargs {
 public:
  static void debug(const char* fmt, ...);
  static void error(const char* fmt, ...);
  static void warn(const char* fmt, ...);
  static void info(const char* fmt, ...);
  static void config(const char* fmt, ...);
  static void fine(const char* fmt, ...);
  static void finer(const char* fmt, ...);
  static void finest(const char* fmt, ...);
};
}

/************************ LOGDEBUG ***********************************/

#define LOGDEBUG                                       \
  if (gemfire::Log::Debug <= gemfire::Log::logLevel()) \
  gemfire::LogVarargs::debug

/************************ LOGERROR ***********************************/

#define LOGERROR                                       \
  if (gemfire::Log::Error <= gemfire::Log::logLevel()) \
  gemfire::LogVarargs::error

/************************ LOGWARN ***********************************/

#define LOGWARN                                          \
  if (gemfire::Log::Warning <= gemfire::Log::logLevel()) \
  gemfire::LogVarargs::warn

/************************ LOGINFO ***********************************/

#define LOGINFO \
  if (gemfire::Log::Info <= gemfire::Log::logLevel()) gemfire::LogVarargs::info

/************************ LOGCONFIG ***********************************/

#define LOGCONFIG                                       \
  if (gemfire::Log::Config <= gemfire::Log::logLevel()) \
  gemfire::LogVarargs::config

/************************ LOGFINE ***********************************/

#define LOGFINE \
  if (gemfire::Log::Fine <= gemfire::Log::logLevel()) gemfire::LogVarargs::fine

/************************ LOGFINER ***********************************/

#define LOGFINER                                       \
  if (gemfire::Log::Finer <= gemfire::Log::logLevel()) \
  gemfire::LogVarargs::finer

/************************ LOGFINEST ***********************************/

#define LOGFINEST                                       \
  if (gemfire::Log::Finest <= gemfire::Log::logLevel()) \
  gemfire::LogVarargs::finest

/******************************************************************************/

/******************************************************************************/

#endif

/******************************************************************************/
