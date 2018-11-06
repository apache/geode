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

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.EventID;

/**
 * Breadcrumbs establishes traces in thread names that are useful in figuring out what is going on
 * in a distributed system given only stack traces.
 *
 * @since GemFire 20 May 2014
 *
 */
public class Breadcrumbs {

  private static ThreadLocal<EventID> EventIDs = new ThreadLocal<EventID>();

  public static boolean ENABLED =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "enable-breadcrumbs");


  /** delimiter for crumb numbers */
  static final String CrumbDelimiter = "/";

  /** string that starts a crumb */
  static final String CommonBreadcrumbStart = "\n\t" + CrumbDelimiter;

  /** all known types of breadcrumbs */
  private enum CrumbType {
    RECEIVE_SIDE, EVENTID, SEND_SIDE, PROBLEM
  }

  /** crumb with the highest ordinal, for initialization of delimiter strings */
  private static CrumbType Crumbiest = CrumbType.PROBLEM;

  private static String[] crumbLabels = new String[] {"rcv", "evt", "snd", "oops"};

  /** strings that start a particular breadcrumb */
  private static String[] crumbStarts = new String[Crumbiest.ordinal() + 1];

  /** strings the terminate a particular breadcrumb */
  private static String[] crumbEnds = new String[Crumbiest.ordinal() + 1];


  static {
    // initialize breadcrumb delimiter strings
    for (int i = 0; i <= Crumbiest.ordinal(); i++) {
      crumbStarts[i] = CommonBreadcrumbStart + crumbLabels[i] + CrumbDelimiter + " ";
      crumbEnds[i] = " " + CrumbDelimiter + crumbLabels[i] + CrumbDelimiter;
    }
  }


  //////////////////////// P U B L I C A P I


  /** set the "sender" breadcrumb for a message while it's being processed */
  public static void setReceiveSide(Object crumb) {
    if (ENABLED) {
      setBreadcrumb(Thread.currentThread(), CrumbType.RECEIVE_SIDE, crumb);
    }
  }

  /** set the "recipients" breadcrumb for a message while waiting for it */
  public static void setSendSide(Object crumb) {
    if (ENABLED) {
      setBreadcrumb(Thread.currentThread(), CrumbType.SEND_SIDE, crumb);
    }
  }

  /** sets the given breadcrumb into the name of the current thread */
  public static void setEventId(Object crumb) {
    if (ENABLED) {
      if (crumb instanceof EventID) {
        EventID other = EventIDs.get();
        if (other != null && other != crumb) {
          ((EventID) crumb).incBreadcrumbCounter();
          setBreadcrumb(Thread.currentThread(), CrumbType.EVENTID, crumb);
        }
      } else {
        setBreadcrumb(Thread.currentThread(), CrumbType.EVENTID, crumb);
      }
    }
  }

  /**
   * a problem crumb can be set using I18n message strings and arguments. Breadcrumb will localize
   * the string with the given args
   */
  public static void setProblem(String msg, Object[] args) {
    if (ENABLED) {
      setBreadcrumb(Thread.currentThread(), CrumbType.PROBLEM, String.format(msg, args));
    }
  }

  /** sets the given breadcrumb into the name of the current thread */
  public static void setProblem(Object crumb) {
    if (ENABLED) {
      setBreadcrumb(Thread.currentThread(), CrumbType.PROBLEM, crumb);
    }
  }

  /** clears the breadcrumbs from the name of the current thread */
  public static void clearBreadcrumb() {
    if (ENABLED) {
      clearBreadcrumb(Thread.currentThread());
      EventIDs.set(null);
    }
  }



  /////////////////////// E N D O F P U B L I C A P I


  /** sets the given breadcrumb into the name of the given thread */
  private static void setBreadcrumb(Thread t, CrumbType type, Object crumb) {
    setCrumbInThread(t, type, crumb);
  }

  /** clears the breadcrumb from the name of the given thread */
  private static void clearBreadcrumb(Thread t) {
    String name = t.getName();
    int i = name.indexOf(CommonBreadcrumbStart);
    if (i >= 0) {
      t.setName(name.substring(0, i));
    }
  }


  /**
   * This method does all of the work of setting/clearing individual breadcrumbs in thread names
   *
   * @param t the thread to modify
   * @param type the type of breadcrumb
   * @param crumb the crumb to insert, or null to clear
   */
  private static void setCrumbInThread(Thread t, CrumbType type, Object crumb) {

    int typeIndex = type.ordinal();
    String name = t.getName();
    String crumbString;

    if (crumb == null) {
      crumbString = ""; // remove existing crumb, if any
    } else {
      crumbString = crumbStarts[typeIndex] + crumb + crumbEnds[typeIndex];
    }

    int startIndex = name.indexOf(crumbStarts[typeIndex]);
    int endIndex = -1;

    if (startIndex < 0) {
      // there may be a higher numbered breadcrumb already in the thread name
      for (int i = typeIndex + 1; (startIndex < 0) && (i <= Crumbiest.ordinal()); i++) {
        startIndex = name.indexOf(crumbStarts[i]);
        if (startIndex >= 0) {
          endIndex = name.indexOf(crumbEnds[i], startIndex + 1);
        }
      }
    }

    if (startIndex < 0) {
      // no equal or higher numbered breadcrumbs - just tack this
      // one onto the end of the thread's name
      t.setName(name + crumbString);
    } else if (endIndex > 0) {
      // insert before the higher-numbered breadcrumb
      t.setName(
          name.substring(0, startIndex) + crumbString + name.substring(startIndex, name.length()));
    } else {
      // replace the existing breadcrumb
      endIndex = name.indexOf(crumbEnds[typeIndex], startIndex + 1);

      // this shouldn't happen
      assert endIndex > 0 : "odd thread name: " + name;

      t.setName(name.substring(0, startIndex) + crumbString
          + name.substring(endIndex + crumbEnds[typeIndex].length(), name.length()));
    }
  }


  private static void clearCrumbInThread(Thread t) {
    String name = t.getName();
    int i = name.indexOf(CommonBreadcrumbStart);
    if (i >= 0) {
      t.setName(name.substring(0, i));
    }
  }

  public static void main(String args[]) {
    String threadName = null;

    ENABLED = true;

    setReceiveSide("processorId=17332");
    threadName = Thread.currentThread().getName();
    System.out.println("thread name with sender=" + threadName);

    setEventId("eventId");
    threadName = Thread.currentThread().getName();
    System.out.println("thread name with eventId=" + threadName);

    setEventId("eventId2");
    threadName = Thread.currentThread().getName();
    System.out.println("replaced eventId: " + threadName);

    setSendSide("recipients");
    threadName = Thread.currentThread().getName();
    System.out.println("with recipients: " + threadName);

    setEventId("eventId3");
    threadName = Thread.currentThread().getName();
    System.out.println("replaced eventId3: " + threadName);

    setSendSide("recipients2");
    threadName = Thread.currentThread().getName();
    System.out.println("with recipients2: " + threadName);

    setReceiveSide("processorId=22222");
    threadName = Thread.currentThread().getName();
    System.out.println("thread name with processor 22222=" + threadName);

    setSendSide(null);
    threadName = Thread.currentThread().getName();
    System.out.println("with recipients removed: " + threadName);

    clearBreadcrumb();
    threadName = Thread.currentThread().getName();
    System.out.println("thread name cleared=" + threadName);
  }

}
