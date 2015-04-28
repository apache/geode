/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date: 2010-11-11 18:48:53 -0800 (Thu, 11 Nov 2010) $
 **/



package com.gemstone.org.jgroups;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Holds version information for JGroups.
 */
public class JGroupsVersion {
    /** changed from 2.2.9.1.2 to 2.2.9.1.3 for gemfire 5.7 */
    /** changed from 2.2.9.1.3 to 2.2.9.1.5 for gemfire 7.0 */
    public static final String description="2.2.9.1.3"; // GemStoneAddition - was 2.2.9.1
    /** changed from 22912 to 22913 for gemfire 5.7 */
    /** changed from 22913 to 22920 for gemfire 7.0 */
    /** changed from 22920 to 22925 for gemfire 7.1, which started incorporating Version info in messages */
    public static final short version=22920; // GemStoneAddition - was 2291
    public static final String cvs="$Id: Version.java,v 1.27 2005/12/27 14:52:48 belaban Exp $";

    public static final byte TOKEN_ORDINAL = -1;
    public static final byte GFE_701_ORDINAL = 20;
    public static final byte GFE_71_ORDINAL = 22;
    public static final byte GFE_80_ORDINAL = 30;
    public static final byte GFE_81_ORDINAL = 35;
    
    public static short CURRENT_ORDINAL = 38; // this is re-initialized by GemFire Version when it loads

    
    public static void writeOrdinal(DataOutput out, short ordinal,
        boolean compressed) throws IOException {
      if (compressed && ordinal <= Byte.MAX_VALUE) {
        out.writeByte(ordinal);
      }
      else {
        out.writeByte(TOKEN_ORDINAL);
        out.writeShort(ordinal);
      }
    }

    /**  /**
   * Reads ordinal as written by {@link #writeOrdinal} from
   * given {@link DataInput}.
   */
  public static short readOrdinal(DataInput in) throws IOException {
    final byte ordinal = in.readByte();
    if (ordinal != TOKEN_ORDINAL) {
      return ordinal;
    }
    else {
      return in.readShort();
    }
  }

    /**
     * Prints the value of the description and cvs fields to System.out.
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("\nVersion: \t" + description);
        System.out.println("CVS: \t\t" + cvs);
        System.out.println("History: \t(see doc/history.txt for details)\n");
    }

    /**
     * Returns the catenation of the description and cvs fields.
     * @return String with description
     */
    public static String printDescription() {
        return "JGroups " + description + "[ " + cvs + "]";
    }

    /**
     * Returns the version field as a String.
     * @return String with version
     */
    public static String printVersion() {
        return new Short(version).toString();
    }

    /**
     * Compares the specified version number against the current version number.
     * @param v short
     * @return Result of == operator.
     */
    public static boolean compareTo(short v) {
        return version == v;
    }
}
