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
package org.apache.geode.gradle.test.dockerized;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;
import com.github.dockerjava.api.model.WaitResponse;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

/**
 * Represents a process executing in a Docker container.
 */
public class DockerProcess extends Process {
  private static final Logger LOGGER = Logging.getLogger(DockerProcess.class);

  private final DockerClient client;
  private final String containerId;
  private final String name;
  private final PipedOutputStream stdIn = new PipedOutputStream();
  private final PipedInputStream stdOut = new PipedInputStream();
  private final PipedInputStream stdErr = new PipedInputStream();
  private final PipedInputStream stdInToContainer = new PipedInputStream(stdIn);
  private final PipedOutputStream stdOutFromContainer = new PipedOutputStream(stdOut);
  private final PipedOutputStream stdErrFromContainer = new PipedOutputStream(stdErr);
  private final AtomicInteger exitCode = new AtomicInteger();
  private final CountDownLatch finished = new CountDownLatch(1);

  /**
   * A callback to copy each frame of the container's output to this wrapper's stdout or stderr.
   */
  private final ResultCallback.Adapter<Frame> onOutputFromContainer = new ResultCallback.Adapter<Frame>() {
    @Override
    public void onNext(Frame frame) {
      byte[] payload = frame.getPayload();
      StreamType streamType = frame.getStreamType();
      try {
        switch (streamType) {
          case STDOUT:
            stdOutFromContainer.write(payload);
            break;
          case STDERR:
            stdErrFromContainer.write(payload);
            break;
          default:
        }
      } catch (IOException e) {
        LOGGER.error("Error while writing to stream:", e);
      }
    }
  };

  /**
   * A callback to execute when the containerized process finishes. Remember the process's exit
   * code, stop watching its streams, close this wrapper's output streams, and stop the container.
   */
  private final ResultCallback.Adapter<WaitResponse> onContainerizedProcessCompletion = new ResultCallback.Adapter<WaitResponse>() {
    @Override
    public void onNext(WaitResponse response) {
      exitCode.set(response.getStatusCode());
      try {
        onOutputFromContainer.close();
        onOutputFromContainer.awaitCompletion();
        stdOutFromContainer.close();
        stdErrFromContainer.close();
      } catch (Exception e) {
        LOGGER.debug("Error while detaching streams", e);
      } finally {
        removeContainer();
      }
    }
  };

  /**
   * Creates a DockerizedProcess that represents a process running in a Docker container.
   * @param client the Docker client that manages the container
   * @param containerId the ID of the container in which the process is running
   * @return a Process that represents the process in the container
   */
  public static Process attachedTo(DockerClient client, String containerId) {
    InspectContainerResponse report = client.inspectContainerCmd(containerId).exec();
    ContainerState state = report.getState();
    if (!isRunning(state)) {
      String message = String.format("Cannot attach to %s because it is %s",
          containerId, state.getStatus());
      throw new RuntimeException(message);
    }
    try {
      DockerProcess process = new DockerProcess(client, containerId, report.getName());
      process.attach();
      return process;
    } catch (Exception e) {
      throw new RuntimeException("Error attaching to " + containerId, e);
    }
  }

  private static boolean isRunning(ContainerState state) {
    Boolean isRunning = state.getRunning();
    return isRunning != null && isRunning;
  }

  private DockerProcess(DockerClient client, String containerId, String name) throws IOException {
    this.client = client;
    this.containerId = containerId;
    this.name = name;
  }

  @Override
  public OutputStream getOutputStream() {
    return stdIn;
  }

  @Override
  public InputStream getInputStream() {
    return stdOut;
  }

  @Override
  public InputStream getErrorStream() {
    return stdErr;
  }

  @Override
  public int waitFor() throws InterruptedException {
    finished.await();
    return exitValue();
  }

  @Override
  public int exitValue() {
    if (finished.getCount() != 0) {
      throw new IllegalThreadStateException("Process in " + this + " is still running");
    }
    return exitCode.get();
  }

  @Override
  public void destroy() {
    client.killContainerCmd(containerId).exec();
  }

  @Override
  public String toString() {
    return String.format("Docker container %s", name);
  }

  /**
   * Attach this wrapper's streams to the container's, and set a callback for when the
   * containerized process finishes.
   * @throws Exception if an error occurs while attaching to the container
   */
  private void attach() throws Exception {
    client.waitContainerCmd(containerId)
        .exec(onContainerizedProcessCompletion);
    if(!client.attachContainerCmd(containerId)
        .withFollowStream(true)
        .withStdOut(true)
        .withStdErr(true)
        .withStdIn(stdInToContainer)
        .exec(onOutputFromContainer)
        .awaitStarted(10, SECONDS)) {
      String message = String.format("Did not attach to %s output within 10 seconds", this);
      LOGGER.warn(message);
      throw new RuntimeException(message);
    }
  }

  private void removeContainer() {
    try {
      client.removeContainerCmd(containerId).exec();
    } catch (Exception e) {
      LOGGER.debug("Error while removing " + this, e);
    } finally {
      finished.countDown();
    }
  }
}
