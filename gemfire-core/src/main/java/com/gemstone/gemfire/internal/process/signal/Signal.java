/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.internal.process.signal;

import com.gemstone.gemfire.internal.lang.StringUtils;

/**
 * Signals defined in the enumerated type were based on Open BSD and the IBM JVM...
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.internal.process.signal.SignalType
 * @since 7.0
 * @link http://www.fromdual.com/operating-system-signals
 * @link http://www.ibm.com/developerworks/java/library/i-signalhandling/#table1
 * @link http://publib.boulder.ibm.com/infocenter/java7sdk/v7r0/index.jsp?topic=%2Fcom.ibm.java.aix.70.doc%2Fuser%2Fsighand.html
 */
@SuppressWarnings("unused")
public enum Signal {
  SIGHUP(1, "HUP", SignalType.INTERRUPT, "Hang up. JVM exits normally."),
  SIGINT(2, "INT", SignalType.INTERRUPT, "Interactive attention (CTRL-C). JVM exits normally."),
  SIGQUIT(3, "QUIT", SignalType.CONTROL, "By default, this triggers a Javadump (or Thread dump)."),
  SIGILL(4, "ILL", SignalType.EXCEPTION, "Illegal instruction (attempt to invoke a unknown machine instruction)."),
  SIGTRAP(5, "TRAP", SignalType.CONTROL, "Used by the JIT."),
  SIGABRT(6, "ABRT", SignalType.ERROR, "Abnormal termination. The JVM raises this signal whenever it detects a JVM fault."),
  SIGEMT(7, "EMT", SignalType.UNKNOWN, "EMT instruction (AIX specific)."),
  SIGFPE(8, "FPE", SignalType.EXCEPTION, "Floating point exception (divide by zero)."),
  SIGKILL(9, "KILL", SignalType.CONTROL, "Kill process."),
  SIGBUS(10, "BUS", SignalType.EXCEPTION, "Bus error (attempt to address nonexistent memory location)."),
  SIGSEGV(11, "SEGV", SignalType.EXCEPTION, "Incorrect access to memory (write to inaccessible memory)."),
  SIGSYS(12, "SYS", SignalType.EXCEPTION, "Bad system call issued."),
  SIGPIPE(13, "PIPE", SignalType.UNKNOWN, "A write to a pipe that is not being read. JVM ignores this."),
  SIGALRM(14, "ALRM", SignalType.CONTROL, "Alarm."),
  SIGTERM(15, "TERM", SignalType.INTERRUPT, "Termination request. JVM will exit normally."),
  SIGURG(16, "URG", SignalType.UNKNOWN, "Unknown"),
  SIGSTOP(17, "STOP", SignalType.UNKNOWN, "Unknown"),
  SIGTSTP(18, "TSTP", SignalType.INTERRUPT, "Ctrl-Y"),
  SIGCONT(19, "CONT", SignalType.UNKNOWN, "Unknown"),
  SIGCHLD(20, "CHLD", SignalType.CONTROL, "Used by the SDK for internal control."),
  SIGTTIN(21, "TTIN", SignalType.UNKNOWN, "Unknown"),
  SIGTTOU(22, "TTOU", SignalType.UNKNOWN, "Unknown"),
  SIGIO(23, "IO", SignalType.UNKNOWN, "Unknown"),
  SIGXCPU(24, "XCPU", SignalType.EXCEPTION, "CPU time limit exceeded (you've been running too long!)."),
  SIGXFSZ(25, "XFSZ", SignalType.EXCEPTION, "File size limit exceeded."),
  SIGVTALRM(26, "VTALRM", SignalType.CONTROL, "Virtual Terminal Alarm?"),
  SIGPROF(27, "PROF", SignalType.UNKNOWN, "Unknown"),
  SIGWINCH(28, "WINCH", SignalType.UNKNOWN, "Unknown"),
  SIGINFO(29, "INFO", SignalType.CONTROL, "Unknown"),
  SIGUSR1(30, "USR1", SignalType.UNKNOWN, "Unknown"),
  SIGUSR2(31, "USR2", SignalType.UNKNOWN, "Unknown");

  private final int number;

  private final SignalType type;

  private final String description;
  private final String name;

  Signal(final int number, final String name, final SignalType type, final String description) {
    assertValidArgument(!StringUtils.isBlank(name), "The name of the signal must be specified!");
    this.number = number;
    this.name = name;
    this.type = type;
    this.description = description;
  }

  protected static void assertValidArgument(final boolean valid, final String message, final Object... arguments) {
    if (!valid) {
      throw new IllegalArgumentException(String.format(message, arguments));
    }
  }

  public static Signal valueOfName(final String name) {
    for (final Signal signal : values()) {
      if (signal.getName().equalsIgnoreCase(name)) {
        return signal;
      }
    }

    return null;
  }

  public static Signal valueOfNumber(final int number) {
    for (final Signal signal : values()) {
      if (signal.getNumber() == number) {
        return signal;
      }
    }

    return null;
  }

  public String getDescription() {
    return description;
  }

  public String getName() {
    return name;
  }

  public int getNumber() {
    return number;
  }

  public SignalType getType() {
    return type;
  }

  @Override
  public String toString() {
    return "SIG".concat(getName());
  }

}
