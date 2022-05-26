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
package org.apache.geode.cache30;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class GreetServer {
  private ServerSocket serverSocket;
  private Socket clientSocket;
  private PrintWriter out;
  private BufferedReader in;

  public void start(int port) throws IOException {
    serverSocket = new ServerSocket(port);
    clientSocket = serverSocket.accept();
    clientSocket.setKeepAlive(true);
    System.out.println("client local port: " + clientSocket.getLocalPort()
        + clientSocket.getRemoteSocketAddress());
    out = new PrintWriter(clientSocket.getOutputStream(), true);
    in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    String greeting = in.readLine();
    if ("hello server".equals(greeting)) {
      out.println("hello client");
      final String[] lsof = {
          "/bin/sh",
          "-c",
          "lsof -i -P -n | grep 6666"
      };

      String output = executeCommand(lsof);
      System.out.println("JC debug: lsof 1: \n" + output);
      serverSocket.close();

      output = executeCommand(lsof);
      System.out.println("JC debug: lsof 2: \n" + output);

      serverSocket = new ServerSocket(port);

      output = executeCommand(lsof);
      System.out.println("JC debug: lsof 3: \n" + output);
    } else {
      out.println("unrecognised greeting");
    }
  }

  public void stop() throws IOException {
    in.close();
    out.close();
    clientSocket.close();
    serverSocket.close();
  }

  private static String executeCommand(final String[] command) {
    final StringBuffer output = new StringBuffer();
    final Process p;
    try {
      p = Runtime.getRuntime().exec(command);
      final BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line = "";
      while ((line = reader.readLine()) != null) {
        output.append(line + "\n");
      }
    } catch (Exception e) {
      System.out.println("JC debug: caught exception when executing shell command:" + e);
    }
    return output.toString();
  }

  public static void main(String[] args) {
    Thread serverThread = new Thread(() -> {
      try {
        GreetServer server = new GreetServer();
        server.start(6666);
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
    serverThread.start();

    try {
      GreetClient client = new GreetClient();
      client.startConnection("127.0.0.1", 6666);
      String response = client.sendMessage("hello server");
      System.out.println(response);

      // final String[] lsof = {
      // "/bin/sh",
      // "-c",
      // "lsof -i -P -n | grep 6666"
      // };
      //
      // String output = executeCommand(lsof);
      // System.out.println("JC debug: lsof 1: " + output);


      Thread.sleep(3000);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }
}
