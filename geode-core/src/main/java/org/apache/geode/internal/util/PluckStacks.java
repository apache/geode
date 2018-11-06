/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import org.apache.geode.management.internal.cli.commands.ExportStackTraceCommand;

/**
 * PluckStacks is a replacement for the old pluckstacks.pl Perl script that we've used for years.
 * When run as a program it takes a list of files that may contain thread dumps. It scans those
 * files, pulls out the thread dumps, removes any well-known idle threads and prints the rest to
 * stdout.
 *
 *
 *
 */

public class PluckStacks {
  static boolean DEBUG = Boolean.getBoolean("PluckStacks.DEBUG");

  // only print one stack dump from each file
  static final boolean ONE_STACK = Boolean.getBoolean("oneDump");

  /**
   * @param args names of files to scan for suspicious threads in thread-dumps
   */
  public static void main(String[] args) throws Exception {
    PluckStacks ps = new PluckStacks();
    for (int i = 0; i < args.length; i++) {
      ps.examineLog(new File(args[i]));
    }
  }

  private void examineLog(File log) throws IOException {

    LineNumberReader reader = null;

    try {
      if (log.getName().endsWith(".gz")) {
        reader = new LineNumberReader(
            new InputStreamReader(new GZIPInputStream(new FileInputStream(log))));
      } else {
        reader = new LineNumberReader(new FileReader(log));
      }
    } catch (FileNotFoundException e) {
      return;
    } catch (IOException e) {
      return;
    }

    try {
      TreeMap<String, List<ThreadStack>> dumps = getThreadDumps(reader, log.getName());

      StringBuffer buffer = new StringBuffer();
      for (Map.Entry<String, List<ThreadStack>> dump : dumps.entrySet()) {
        if (dump.getValue().size() > 0) {
          buffer.append(dump.getKey());
          for (ThreadStack stack : dump.getValue()) {
            stack.appendToBuffer(buffer);
            buffer.append("\n");
          }
          buffer.append("\n\n");
        }
        if (ONE_STACK) {
          break;
        }
      }
      String output = buffer.toString();
      if (output.length() > 0) {
        System.out.println(output);
      }
    } finally {
      reader.close();
    }
  }

  public TreeMap<String, List<ThreadStack>> getThreadDumps(LineNumberReader reader,
      String logFileName) {
    TreeMap<String, List<ThreadStack>> result = new TreeMap<>();

    String line = null;
    int stackNumber = 1;
    try {
      while ((line = reader.readLine()) != null) {
        if (line.startsWith("Full thread dump")
            || line.startsWith(ExportStackTraceCommand.STACK_TRACE_FOR_MEMBER)) {
          int lineNumber = reader.getLineNumber();
          List<ThreadStack> stacks = getStacks(reader);
          if (stacks.size() > 0) {
            StringBuffer buffer = new StringBuffer();
            buffer.append("[Stack #").append(stackNumber++)
                .append(" from " + logFileName + " line " + lineNumber + "]\n").append(line)
                .append("\n");
            result.put(buffer.toString(), stacks);
          }
          if (ONE_STACK) {
            break;
          }
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Something went wrong processing " + logFileName, ioe);
    }
    return result;
  }

  /** parses each stack trace and returns any that are unexpected */
  public List<ThreadStack> getStacks(BufferedReader reader) throws IOException {
    List<ThreadStack> result = new LinkedList<ThreadStack>();
    ThreadStack lastStack = null;
    ArrayList breadcrumbs = new ArrayList(4);
    do {
      String line = null;
      // find the start of the stack
      do {
        reader.mark(100000);
        if ((line = reader.readLine()) == null) {
          break;
        }
        if (line.startsWith(ExportStackTraceCommand.STACK_TRACE_FOR_MEMBER)) {
          reader.reset();
          Collections.sort(result);
          return result;
        }
        if (line.length() > 0 && line.charAt(0) == '"')
          break;
        if (lastStack != null) {
          if (line.length() == 0 || line.charAt(0) == '\t') {
            lastStack.add(line);
          }
        }
      } while (true);
      // cache the first two lines and examine the third to see if it starts with a tab and "at "
      String firstLine = line;
      String secondLine = null;
      breadcrumbs.clear();
      do {
        line = reader.readLine();
        if (line == null) {
          Collections.sort(result);
          return result;
        }
        if (line.startsWith("\t/")) {
          breadcrumbs.add(line);
          continue;
        } else {
          break;
        }
      } while (true);
      secondLine = line;
      line = reader.readLine();
      if (line == null) {
        Collections.sort(result);
        return result;
      }

      lastStack = new ThreadStack(firstLine, secondLine, line, reader);
      lastStack.addBreadcrumbs(breadcrumbs);
      if (DEBUG) {
        if (breadcrumbs.size() > 0) {
          System.out.println(
              "added " + breadcrumbs.size() + " breadcrumbs to " + lastStack.getThreadName());
        }
        System.out.println("examining thread " + lastStack.getThreadName());
      }
      if (!isExpectedStack(lastStack)) {
        result.add(lastStack);
      }
      if (lastStack.getThreadName().equals("VM Thread")) {
        Collections.sort(result);
        return result;
      }
    } while (true);
  }

  /* see if this thread is at an expected pause point */
  boolean isExpectedStack(ThreadStack thread) {
    String threadName = thread.getThreadName();
    int stackSize = thread.size();

    // keep all hydra threads
    if (threadName.startsWith("vm_"))
      return false;

    // these are never interesting to me
    if (threadName.startsWith("StatDispatcher"))
      return true;
    if (threadName.startsWith("State Logger Consumer Thread"))
      return true;
    if (threadName.contains("StatSampler"))
      return true;
    if (threadName.startsWith("IDLE p2pDestreamer"))
      return true;
    if (threadName.startsWith("Idle OplogCompactor"))
      return true;


    ////////////// HIGH FREQUENCY STACKS //////////////////////

    // check these first for efficiency

    if (threadName.startsWith("Cache Client Updater Thread")) {
      return stackSize == 13 && thread.get(2).contains("SocketInputStream.socketRead0");
    }
    if (threadName.startsWith("Client Message Dispatcher")) {
      return stackSize == 13 && thread.get(1).contains("TIMED_WAITING");
    }
    if (threadName.startsWith("Function Execution Processor")) {
      return isIdleExecutor(thread);
    }
    if (threadName.startsWith("Geode Failure Detection Server")) {
      return stackSize < 12 && thread.getFirstFrame().contains("socketAccept");
    }
    if (threadName.startsWith("Geode Membership Timer")) {
      // System.out.println("gf timer stack size = " + stackSize + "; frame = " + thread.get(1));
      return stackSize < 9 && !thread.isRunnable();
    }
    if (threadName.startsWith("Geode Membership View Creator")) {
      // System.out.println("gf view creator stack size = " + stackSize + "; frame = " +
      // thread.get(1));
      return stackSize < 8 && !thread.isRunnable();
    }
    if (threadName.startsWith("Geode Heartbeat Sender")) {
      return stackSize <= 8 && !thread.isRunnable();
    }
    // thread currently disabled
    // if (threadName.startsWith("Geode Suspect Message Collector")) {
    // return stackSize <= 7 && thread.get(1).contains("Thread.State: WAITING");
    // }
    if (threadName.startsWith("multicast receiver")) {
      return (stackSize > 2 && thread.get(2).contains("PlainDatagramSocketImpl.receive"));
    }
    if (threadName.startsWith("P2P Listener")) {
      // System.out.println("p2p listener stack size = " + stackSize + "; frame = " +
      // thread.get(2));
      return (stackSize == 8 && thread.get(2).contains("SocketChannelImpl.accept"));
    }
    if (threadName.startsWith("P2P message reader")) {
      return (stackSize <= 14 && (thread.getFirstFrame().contains("FileDispatcherImpl.read")
          || thread.getFirstFrame().contains("FileDispatcher.read")
          || thread.getFirstFrame().contains("SocketDispatcher.read")));
    }
    if (threadName.startsWith("PartitionedRegion Message Processor")) {
      return isIdleExecutor(thread);
    }
    if (threadName.startsWith("Pooled Message Processor")) {
      return isIdleExecutor(thread);
    }
    if (threadName.startsWith("Pooled High Priority Message Processor")) {
      return isIdleExecutor(thread) || thread.get(4).contains("OSProcess.zipStacks");
    }
    if (threadName.startsWith("Pooled Serial Message Processor")) {
      return isIdleExecutor(thread);
    }
    if (threadName.startsWith("Pooled Waiting Message Processor")) {
      return isIdleExecutor(thread);
    }
    if (threadName.startsWith("ServerConnection")) {
      if (thread.getFirstFrame().contains("socketRead")
          && (stackSize > 6 && thread.get(6).contains("fetchHeader")))
        return true; // reading from a client
      return isIdleExecutor(thread);
    }
    if (threadName.startsWith("TCP Check ServerSocket Thread")) {
      return (stackSize >= 3 && thread.get(2).contains("socketAccept"));
    }
    if (threadName.startsWith("Timer runner")) {
      // System.out.println("timer runner stack size = " + stackSize + "; frame = " +
      // thread.get(1));
      return (stackSize <= 10 && thread.get(1).contains("TIMED_WAITING"));
    }
    if (threadName.startsWith("TransferQueueBundler")) {
      // System.out.println("transfer bundler stack size = " + stackSize + "; frame = " +
      // thread.get(2));
      return (stackSize == 9 && thread.get(2).contains("sun.misc.Unsafe.park"));
    }
    if (threadName.startsWith("unicast receiver")) {
      // System.out.println("unicast receiver stack size = " + stackSize + "; frame = " +
      // thread.get(3));
      return (stackSize > 2 && thread.get(2).contains("PlainDatagramSocketImpl.receive"));
    }

    /////////////////////////////////////////////////////////////////////////

    // Now check the rest of the product threads in alphabetical order

    if (threadName.startsWith("Asynchronous disk writer")) {
      return !thread.isRunnable() && stackSize <= 10
          && (stackSize >= 7 && (thread.get(5).contains("waitUntilFlushIsReady")
              || thread.get(6).contains("waitUntilFlushIsReady")));
    }
    if (threadName.startsWith("BridgeServer-LoadPollingThread")) {
      return !thread.isRunnable() && stackSize == 5;
    }
    if (threadName.startsWith("Cache Server Acceptor")) {
      return (thread.getFirstFrame().contains("socketAccept"));
    }
    if (threadName.startsWith("Client Message Dispatcher")) {
      return (stackSize == 11 && thread.getFirstFrame().contains("socketWrite"))
          || (stackSize == 14 && thread.getFirstFrame().contains("Unsafe.park"));
    }
    if (threadName.equals("ClientHealthMonitor Thread")) {
      return !thread.isRunnable() && stackSize == 4;
    }
    if (threadName.startsWith("Distribution Locator on")) {
      return stackSize <= 9 && thread.getFirstFrame().contains("socketAccept");
    }
    if (threadName.startsWith("DM-MemberEventInvoker")) {
      return !thread.isRunnable() && (stackSize == 9 && thread.get(6).contains("Queue.take"));
    }
    if (threadName.startsWith("Event Processor for GatewaySender")) {
      return !thread.isRunnable()
          && thread.get(3).contains("ConcurrentParallelGatewaySenderQueue.peek");
    }
    if (threadName.startsWith("GC Daemon")) {
      return !thread.isRunnable() && stackSize <= 6;
    }
    if (threadName.startsWith("Replicate/Partition Region Garbage Collector")) {
      return !thread.isRunnable() && (stackSize <= 9);
    }
    if (threadName.startsWith("Non-replicate Region Garbage Collector")) {
      return !thread.isRunnable() && (stackSize <= 9);
    }
    if (threadName.equals("GemFire Time Service")) {
      return !thread.isRunnable();
    }
    if (threadName.startsWith("GlobalTXTimeoutMonitor")) {
      return !thread.isRunnable()
          && (stackSize <= 8 && thread.getFirstFrame().contains("Object.wait"));
    }
    if (threadName.startsWith("JoinProcessor")) {
      return !thread.isRunnable() && stackSize <= 7;
    }
    if (threadName.startsWith("locator request thread")) {
      return isIdleExecutor(thread);
    }
    if (threadName.startsWith("Lock Grantor for")) {
      return !thread.isRunnable() && stackSize <= 8;
    }
    if (threadName.startsWith("Management Task")) {
      return isIdleExecutor(thread);
    }
    if (threadName.startsWith("osprocess reaper")) {
      return (stackSize == 5);
    }
    if (threadName.startsWith("P2P-Handshaker")) {
      return isIdleExecutor(thread);
    }
    if (threadName.startsWith("P2P Listener")) {
      return (stackSize == 7 && thread.getFirstFrame().contains("accept0"));
    }
    if (threadName.startsWith("Queue Removal Thread")) {
      return !thread.isRunnable()
          && (stackSize == 5 && thread.getFirstFrame().contains("Object.wait"));
    }
    if (threadName.startsWith("ResourceManagerRecoveryThread")) {
      return isIdleExecutor(thread);
    }
    if (threadName.startsWith("RMI TCP Connection(idle)")) {
      return true;
    }
    if (threadName.startsWith("RMI Reaper")) {
      return true;
    }
    if (threadName.startsWith("RMI RenewClean")) {
      return true;
    }
    if (threadName.startsWith("RMI Scheduler")) {
      return isIdleExecutor(thread);
    }
    if (threadName.startsWith("RMI TCP Accept")) {
      return true;
    }
    if (threadName.startsWith("RMI TCP Connection")) {
      return thread.getFirstFrame().contains("socketRead0");
    }
    if (threadName.startsWith("SnapshotResultDispatcher")) {
      return !thread.isRunnable() && stackSize <= 8;
    }
    if (threadName.startsWith("StatMonitorNotifier Thread")) {
      return (stackSize > 8 && thread.get(7).contains("SynchronousQueue.take"));
    }
    if (threadName.startsWith("SystemFailure Proctor")) {
      return !thread.isRunnable()
          && (stackSize == 6 && thread.getFirstFrame().contains("Thread.sleep"));
    }
    if (threadName.startsWith("SystemFailure WatchDog")) {
      return (stackSize <= 8 && thread.getFirstFrame().contains("Object.wait"));
    }
    if (threadName.startsWith("ThresholdEventProcessor")) {
      return isIdleExecutor(thread);
    }
    if (threadName.startsWith("ThreadsMonitor") && thread.getFirstFrame().contains("Object.wait")) {
      return true;
    }
    if (threadName.startsWith("Timer-")) {
      if (thread.isRunnable())
        return true;
      if ((stackSize <= 8) && thread.getFirstFrame().contains("Object.wait"))
        return true;
    }
    if (threadName.startsWith("TimeScheduler.Thread")) {
      return !thread.isRunnable()
          && (stackSize <= 8 && thread.getFirstFrame().contains("Object.wait"));
    }
    if (threadName.startsWith("vfabric-license-heartbeat")) {
      if (thread.isRunnable())
        return false;
      if (stackSize == 6 && thread.getFirstFrame().contains("Thread.sleep"))
        return true;
      return (stackSize <= 7 && thread.getFirstFrame().contains("Object.wait"));
    }
    if (threadName.equals("WAN Locator Discovery Thread")) {
      return (!thread.isRunnable() && thread.get(3).contains("exchangeRemoteLocators"));
    }
    return isIdleExecutor(thread);
  }


  boolean isIdleExecutor(ThreadStack thread) {
    if (thread.isRunnable()) {
      return false;
    }
    int size = thread.size();
    if (size > 8 && thread.get(7).contains("DMStats.take")) {
      return true;
    }
    for (int i = 3; i < 12; i++) {
      if (size > i && thread.get(size - i).contains("getTask")) {
        return true;
      }
    }
    return false;
  }


  /** ThreadStack holds the stack for a single Java Thread */
  public static class ThreadStack implements Comparable {
    List<String> lines = new ArrayList<String>(20);
    boolean runnable;
    List<String> breadcrumbs;

    /** create a stack with the given initial lines, reading the rest from the Reader */
    ThreadStack(String firstLine, String secondLine, String thirdLine, BufferedReader reader)
        throws IOException {
      lines.add(firstLine);
      lines.add(secondLine);
      runnable = secondLine.contains("RUNNABLE");
      lines.add(thirdLine);

      String line = null;
      do {
        reader.mark(100000);
        line = reader.readLine();
        if (line == null || line.trim().length() == 0) {
          break;
        }
        if (line.startsWith("\"")) {
          reader.reset();
          break;
        }
        lines.add(line);
      } while (true);
    }

    void addBreadcrumbs(List crumbs) {
      this.breadcrumbs = new ArrayList<String>(crumbs);
    }

    void add(String line) {
      lines.add(line);
    }

    String get(int position) {
      return lines.get(position);
    }

    boolean isRunnable() {
      return runnable;
    }

    boolean contains(String subString) {
      for (int i = lines.size() - 1; i >= 0; i--) {
        if (lines.get(i).contains(subString)) {
          return true;
        }
      }
      return false;
    }

    String getFirstFrame() {
      if (lines.size() > 2) {
        return lines.get(2);
      } else {
        return "";
      }
    }

    String getThreadName() {
      String firstLine = lines.get(0);
      int quote = firstLine.indexOf('"', 1);
      if (quote > 1) {
        return firstLine.substring(1, quote);
      }
      return firstLine.substring(1, firstLine.length());
    }

    int size() {
      return lines.size();
    }

    @Override
    public String toString() {
      StringWriter sw = new StringWriter();
      boolean first = true;
      for (String line : lines) {
        sw.append(line).append("\n");
        if (first && this.breadcrumbs != null) {
          for (String bline : breadcrumbs) {
            sw.append(bline).append("\n");
          }
        }
        first = false;
      }
      return sw.toString();
    }

    public void writeTo(Writer w) throws IOException {
      if (DEBUG) {
        w.append("stack.name='" + getThreadName() + "' runnable=" + this.runnable + " lines="
            + lines.size());
        w.append("\n");
      }
      boolean first = true;
      for (String line : lines) {
        w.append(line);
        w.append("\n");
        if (first) {
          first = false;
          if (breadcrumbs != null) {
            for (String bline : breadcrumbs) {
              w.append(bline).append("\n");
            }
          }
        }
      }
    }

    public void appendToBuffer(StringBuffer buffer) {
      if (DEBUG)
        buffer.append("stack.name='" + getThreadName() + "' runnable=" + this.runnable + " lines="
            + lines.size()).append("\n");
      boolean first = true;
      for (String line : lines) {
        buffer.append(line).append("\n");
        if (first && breadcrumbs != null) {
          for (String bline : breadcrumbs) {
            buffer.append(bline).append("\n");
          }
        }
        first = false;
      }
    }

    @Override
    public int compareTo(Object other) {
      return ((ThreadStack) other).getThreadName().compareTo(getThreadName());
    }
  }

}
