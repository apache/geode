/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.memcached;

/**
 * Represents the reply messages sent to the client.
 * All reply types override toString to send "\r\n"
 * @author Swapnil Bawaskar
 */
public enum Reply {
  
  /**
   * to indicate success
   */
  STORED {
    @Override
    public String toString() {
      return "STORED\r\n";
    }
  },
  
  /**
   * to indicate the data was not stored, but not
   * because of an error. This normally means that the
   * condition for an "add" or a "replace" command wasn't met.
   */
  NOT_STORED {
    @Override
    public String toString() {
      return "NOT_STORED\r\n";
    }
  },
  
  /**
   * to indicate that the item you are trying to store with
   * a "cas" command has been modified since you last fetched it.
   */
  EXISTS {
    @Override
    public String toString() {
      return "EXISTS\r\n";
    }
  },
  
  /**
   * to indicate that the item you are trying to store
   * with a "cas" command did not exist.
   * Also used by delete.
   */
  NOT_FOUND {
    @Override
    public String toString() {
      return "NOT_FOUND\r\n";
    }
  },
  
  /**
   * to indicate that get/gets operation has completed
   */
  END {
    @Override
    public String toString() {
      return "END\r\n";
    }
  },
  
  /**
   * to indicate that flush_all has completed
   */
  OK {
    @Override
    public String toString() {
      return "OK\r\n";
    }
  },
  
  /**
   * to indicate success on delete
   */
  DELETED {
    @Override
    public String toString() {
      return "DELETED\r\n";
    }
  },

  /**
   * means the client sent a nonexistent command name
   */
  ERROR {
    @Override
    public String toString() {
      return "ERROR\r\n";
    }
  },

  /**
   * means some sort of client error in the input line
   */
  CLIENT_ERROR {
    @Override
    public String toString() {
      return "CLIENT_ERROR client error in the input line\r\n";
    }
  }
}

