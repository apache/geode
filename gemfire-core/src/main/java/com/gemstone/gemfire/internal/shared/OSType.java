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

package com.gemstone.gemfire.internal.shared;

/**
 * Enumeration for various types of OSes supported by {@link NativeCalls} using
 * JNA ({@link NativeCallsJNAImpl}).
 */
public enum OSType {

  /** Indicates a Linux family OS. */
  LINUX {
    @Override
    public boolean isPOSIX() {
      return true;
    }
  },

  /** Indicates a Solaris family OS. */
  SOLARIS {
    @Override
    public boolean isPOSIX() {
      return true;
    }
  },

  /** Indicates a MacOSX family OS. */
  MACOSX {
    @Override
    public boolean isPOSIX() {
      return true;
    }
  },

  /** Indicates a FreeBSD family OS. */
  FREEBSD {
    @Override
    public boolean isPOSIX() {
      return true;
    }
  },

  /**
   * Indicates a generic POSIX complaint OS (at least to a reasonable degree).
   */
  GENERIC_POSIX {
    @Override
    public boolean isPOSIX() {
      return true;
    }
  },

  /**
   * Indicates a Microsoft Windows family OS.
   */
  WIN,

  /**
   * Indicates an OS whose kind cannot be determined or that is not supported by
   * JNA.
   */
  GENERIC;

  /**
   * Indicates a Microsoft Windows family OS.
   */
  public final boolean isWindows() {
    return this == WIN;
  }

  /**
   * Indicates an OS that conforms to POSIX specifications (at least to a
   * reasonable degree).
   */
  public boolean isPOSIX() {
    return false;
  }
}
