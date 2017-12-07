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
package org.apache.geode.internal.cache.client.protobuf;

import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.StringReader;

import org.apache.geode.annotations.Experimental;

@Experimental
public class ProtobufClientRunner extends MethodInvoker {
  private static class FilePrompterAndTranscriber implements Prompter, Transcriber {
    private final String path;
    private int line = 0;

    private FilePrompterAndTranscriber(File file) {
      path = file.getAbsolutePath();
    }

    public void prompt() {
      ++line;
    }

    public void transcribe(String str) {
      System.out.println("# File " + path + ", line " + line + ": " + str);
    }
  }

  private CacheFactory cacheFactory = null;
  private Cache cache = null;
  private Region<Integer, String> region = null;
  private int line = 0;

  public static void main(String[] args) {
    try {
      ProtobufClientRunner runner = new ProtobufClientRunner();
      runner.invoke(args);
    } catch (Throwable t) {
      t.printStackTrace();
      System.exit(1);
    }
  }

  public void invoke(String[] args) throws Throwable {
    if (args.length < 1) {
      invoke(new InputStreamReader(System.in), this, () -> System.out.print("> "), (str) -> {
      });
    } else {
      final File file = new File(args[0]);
      if (file.exists() && !file.isDirectory()) {
        FilePrompterAndTranscriber filePrompterAndTranscriber =
            new FilePrompterAndTranscriber(file);
        invoke(new FileReader(file), this, filePrompterAndTranscriber, filePrompterAndTranscriber);
      } else {
        StringBuilder builder = new StringBuilder();
        for (String arg : args) {
          if (arg.contains(" ") || arg.contains("\t")) {
            arg = arg.replace(" ", "\\ ");
            arg = arg.replace("\t", "\\\t");
          }
          if (0 < builder.length()) {
            builder.append(' ');
          }
          builder.append(arg);
        }
        invoke(new StringReader(builder.toString()), this);
      }
    }
  }

  public void quit() {
    System.exit(0);
  }

  public void addLocator(String host, int port) {
    cacheFactory = new CacheFactory().addLocator(host, port);
  }

  public void getAvailableServers() throws Exception {
    if (cacheFactory == null) {
      throw new Exception("No locator");
    }

    for (CacheFactory.ServerInfo server : cacheFactory.getAvailableServers()) {
      System.out.println(server.host + "[" + server.port + "]");
    }
  }

  public void connect() throws Exception {
    cache = cacheFactory.connect();
  }

  public void getRegion(String name) {
    region = cache.getRegion(name);
  }
}
