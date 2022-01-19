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
package org.apache.geode.internal.memcached;

import java.nio.ByteBuffer;

import org.apache.geode.internal.memcached.commands.AddCommand;
import org.apache.geode.internal.memcached.commands.AddQCommand;
import org.apache.geode.internal.memcached.commands.AppendCommand;
import org.apache.geode.internal.memcached.commands.AppendQCommand;
import org.apache.geode.internal.memcached.commands.CASCommand;
import org.apache.geode.internal.memcached.commands.DecrementCommand;
import org.apache.geode.internal.memcached.commands.DecrementQCommand;
import org.apache.geode.internal.memcached.commands.DeleteCommand;
import org.apache.geode.internal.memcached.commands.DeleteQCommand;
import org.apache.geode.internal.memcached.commands.FlushAllCommand;
import org.apache.geode.internal.memcached.commands.FlushAllQCommand;
import org.apache.geode.internal.memcached.commands.GATCommand;
import org.apache.geode.internal.memcached.commands.GATQCommand;
import org.apache.geode.internal.memcached.commands.GetCommand;
import org.apache.geode.internal.memcached.commands.GetKCommand;
import org.apache.geode.internal.memcached.commands.GetKQCommand;
import org.apache.geode.internal.memcached.commands.GetQCommand;
import org.apache.geode.internal.memcached.commands.IncrementCommand;
import org.apache.geode.internal.memcached.commands.IncrementQCommand;
import org.apache.geode.internal.memcached.commands.NoOpCommand;
import org.apache.geode.internal.memcached.commands.NotSupportedCommand;
import org.apache.geode.internal.memcached.commands.PrependCommand;
import org.apache.geode.internal.memcached.commands.PrependQCommand;
import org.apache.geode.internal.memcached.commands.QuitCommand;
import org.apache.geode.internal.memcached.commands.QuitQCommand;
import org.apache.geode.internal.memcached.commands.ReplaceCommand;
import org.apache.geode.internal.memcached.commands.ReplaceQCommand;
import org.apache.geode.internal.memcached.commands.SetCommand;
import org.apache.geode.internal.memcached.commands.SetQCommand;
import org.apache.geode.internal.memcached.commands.StatsCommand;
import org.apache.geode.internal.memcached.commands.TouchCommand;
import org.apache.geode.internal.memcached.commands.VerbosityCommand;
import org.apache.geode.internal.memcached.commands.VersionCommand;

/**
 * Represents all commands a memcached client can issue
 *
 */
public enum Command {

  SET {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new SetCommand();
      }
      return processor;
    }
  },
  SETQ {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new SetQCommand();
      }
      return processor;
    }
  },
  ADD {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new AddCommand();
      }
      return processor;
    }
  },
  ADDQ {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new AddQCommand();
      }
      return processor;
    }
  },
  REPLACE {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new ReplaceCommand();
      }
      return processor;
    }
  },
  REPLACEQ {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new ReplaceQCommand();
      }
      return processor;
    }
  },
  APPEND {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new AppendCommand();
      }
      return processor;
    }
  },
  APPENDQ {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new AppendQCommand();
      }
      return processor;
    }
  },
  PREPEND {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new PrependCommand();
      }
      return processor;
    }
  },
  PREPENDQ {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new PrependQCommand();
      }
      return processor;
    }
  },
  CAS {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new CASCommand();
      }
      return processor;
    }
  },
  GET {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new GetCommand();
      }
      return processor;
    }
  },
  GETS {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new GetCommand();
      }
      return processor;
    }
  },
  GETQ {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new GetQCommand();
      }
      return processor;
    }
  },
  GETK {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new GetKCommand();
      }
      return processor;
    }
  },
  GETKQ {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new GetKQCommand();
      }
      return processor;
    }
  },
  DELETE {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new DeleteCommand();
      }
      return processor;
    }
  },
  DELETEQ {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new DeleteQCommand();
      }
      return processor;
    }
  },
  INCR {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new IncrementCommand();
      }
      return processor;
    }
  },
  INCRQ {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new IncrementQCommand();
      }
      return processor;
    }
  },
  DECR {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new DecrementCommand();
      }
      return processor;
    }
  },
  DECRQ {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new DecrementQCommand();
      }
      return processor;
    }
  },
  STATS {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new StatsCommand();
      }
      return processor;
    }
  },
  FLUSH_ALL {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new FlushAllCommand();
      }
      return processor;
    }
  },
  FLUSH_ALLQ {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new FlushAllQCommand();
      }
      return processor;
    }
  },
  VERSION {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new VersionCommand();
      }
      return processor;
    }
  },
  VERBOSITY {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new VerbosityCommand();
      }
      return processor;
    }
  },
  QUIT {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new QuitCommand();
      }
      return processor;
    }
  },
  QUITQ {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new QuitQCommand();
      }
      return processor;
    }
  },
  NOOP {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new NoOpCommand();
      }
      return processor;
    }
  },
  TOUCH {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new TouchCommand();
      }
      return processor;
    }
  },
  GAT {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new GATCommand();
      }
      return processor;
    }
  },
  GATQ {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new GATQCommand();
      }
      return processor;
    }
  },
  NOT_SUPPORTED {
    private CommandProcessor processor;

    @Override
    public CommandProcessor getCommandProcessor() {
      if (processor == null) {
        processor = new NotSupportedCommand();
      }
      return processor;
    }

  };

  /**
   * Returns a command processor to process the given {@link #Command}
   *
   * @return a command processor
   */
  public abstract CommandProcessor getCommandProcessor();

  public static String buffertoString(ByteBuffer header) {
    StringBuilder str = new StringBuilder("\n0: ");
    for (int i = 0; i < header.limit(); i++) {
      str.append(byteToHex(header.get(i)) + " | ");
      if ((i + 1) % 4 == 0) {
        str.append("\n");
        str.append((i + 1) + ": ");
      }
    }
    return str.toString();
  }

  public static Command getCommandFromOpCode(byte opCode) {
    switch (opCode) {
      case 0x00:
        return GET;
      case 0x01:
        return SET;
      case 0x02:
        return ADD;
      case 0x03:
        return REPLACE;
      case 0x04:
        return DELETE;
      case 0x05:
        return INCR;
      case 0x06:
        return DECR;
      case 0x07:
        return QUIT;
      case 0x08:
        return FLUSH_ALL;
      case 0x09:
        return GETQ;
      case 0x0a:
        return NOOP;
      case 0x0b:
        return VERSION;
      case 0x0c:
        return GETK;
      case 0x0d:
        return GETKQ;
      case 0x0e:
        return APPEND;
      case 0x0f:
        return PREPEND;
      case 0x10:
        return STATS;
      case 0x11:
        return SETQ;
      case 0x12:
        return ADDQ;
      case 0x13:
        return REPLACEQ;
      case 0x14:
        return DELETEQ;
      case 0x15:
        return INCRQ;
      case 0x16:
        return DECRQ;
      case 0x17:
        return QUITQ;
      case 0x18:
        return FLUSH_ALLQ;
      case 0x19:
        return APPENDQ;
      case 0x1a:
        return PREPENDQ;
      case 0x1b:
        return VERBOSITY;
      case 0x1c:
        return TOUCH;
      case 0x1d:
        return GAT;
      case 0x1e:
        return GATQ;
      default:
        return NOT_SUPPORTED;
    }
  }

  public static String byteToHex(byte b) {
    // Returns hex String representation of byte b
    char[] hexDigit =
        {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    char[] array = {hexDigit[(b >> 4) & 0x0f], hexDigit[b & 0x0f]};
    return "0x" + new String(array);
  }
}
