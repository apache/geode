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
package org.apache.geode.gradle.testing.dockerized;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.gradle.testing.dockerized.DockerTestWorkerConfig.getDurationWarningThreshold;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.async.ResultCallbackTemplate;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.StreamType;
import com.github.dockerjava.api.model.WaitResponse;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.internal.UncheckedException;

/**
 * Represents a process running in a Docker container.
 */
public class DockerProcess extends Process {
  private static final Logger LOGGER = Logging.getLogger(DockerProcess.class);

  private final String owner;
  private final DockerClient client;
  private final String containerId;
  private final int timeoutMillis;
  private final Runnable onCompletion;
  private final PipedOutputStream stdIn = new PipedOutputStream();
  private final PipedInputStream stdOut = new PipedInputStream();
  private final PipedInputStream stdErr = new PipedInputStream();
  private final PipedInputStream stdInToContainer = new PipedInputStream();
  private final PipedOutputStream stdOutFromContainer = new PipedOutputStream();
  private final PipedOutputStream stdErrFromContainer = new PipedOutputStream();
  private final AtomicInteger exitCode = new AtomicInteger();
  private final CountDownLatch finished = new CountDownLatch(1);
  private final OutputListener outputListener = new OutputListener();
  private final TerminationListener terminationListener = new TerminationListener();

  /**
   * Creates a {@link Process} that represents a process running in a Docker container.
   *
   * @param owner the name of this process's owner (used for diagnostics)
   * @param client a Docker client to use to listen for process output and termination
   * @param containerId the ID of the container in which the process is running
   * @param timeoutMillis duration to wait for each listener to start
   * @param onCompletion a runnable to run when this process completes
   * @return a Process that represents the process in the container
   */
  public static Process attachedTo(String owner, DockerClient client, String containerId,
      int timeoutMillis, Runnable onCompletion) {
    DockerProcess process = new DockerProcess(
        owner, client, containerId, timeoutMillis, onCompletion);
    try {
      process.attach();
    } catch (Exception e) {
      UncheckedException.throwAsUncheckedException(e);
    }
    return process;
  }

  private DockerProcess(String owner, DockerClient client, String containerId, int timeoutMillis,
      Runnable onCompletion) {
    this.owner = owner;
    this.client = client;
    this.containerId = containerId;
    this.timeoutMillis = timeoutMillis;
    this.onCompletion = onCompletion;
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
      throw new IllegalThreadStateException(toString() + " is still running");
    }
    return exitCode.get();
  }

  @Override
  public void destroy() {
    finish();
  }

  @Override
  public String toString() {
    return String.format("DockerProcess{%s:%s}", owner, containerId);
  }

  /**
   * Attach this {@code DockerProcess}'s input and output streams to the container's, and set a
   * callback for when the containerized process finishes.
   *
   * @throws Exception if an error occurs while attaching to the container
   */
  private void attach() throws Exception {
    listenForTermination();
    connectStreams();
    listenForOutput();
  }

  private void listenForOutput() throws InterruptedException {
    LOGGER.debug("{} installing {}", this, outputListener);
    try {
      long startTime = System.currentTimeMillis();
    client.attachContainerCmd(containerId)
        .withFollowStream(true)
        .withStdOut(true)
        .withStdErr(true)
        .withStdIn(stdInToContainer)
        .exec(outputListener);
      LOGGER.debug("{} installed {}", this, outputListener);
      long duration = System.currentTimeMillis() - startTime;
      if(duration > getDurationWarningThreshold()) {
        LOGGER.warn("{} {} installation took {}ms", this, outputListener, duration);
      }
    } catch (RuntimeException e) {
      String message = String.format("%s error while installing %s", this, outputListener);
      throw new RuntimeException(message, e);
    }
    waitUntilStarted(outputListener);
  }

  private void connectStreams() throws IOException {
    stdInToContainer.connect(stdIn);
    stdOutFromContainer.connect(stdOut);
    stdErrFromContainer.connect(stdErr);
  }

  private void listenForTermination() throws InterruptedException {
    LOGGER.debug("{} installing {}", this, terminationListener);
    try {
      long startTime = System.currentTimeMillis();
      client.waitContainerCmd(containerId)
          .exec(terminationListener);
      LOGGER.debug("{} installed {}", this, terminationListener);
      long duration = System.currentTimeMillis() - startTime;
      if(duration > getDurationWarningThreshold()) {
        LOGGER.warn("{} {} installation took {}ms", this, terminationListener, duration);
      }
    } catch (RuntimeException e) {
      String message = String.format("%s error while installing %s", this, terminationListener);
      throw new RuntimeException(message, e);
    }
    waitUntilStarted(terminationListener);
  }

  private void waitUntilStarted(ResultCallbackTemplate<?, ?> listener)
      throws InterruptedException {
    if (timeoutMillis > 0) {
      LOGGER.debug("{} waiting {}ms for {} to start", this, timeoutMillis, listener);
      if (!listener.awaitStarted(timeoutMillis, MILLISECONDS)) {
        String message = String.format(
            "%s timed out after %dms waiting for %s to start", this, timeoutMillis, listener);
        throw new RuntimeException(message);
      }
    } else {
      LOGGER.debug("{} waiting for {} to start", this, listener);
      listener.awaitStarted();
    }
    LOGGER.debug("{} {} started", this, listener);
  }

  private void finish() {
    close("stdin", stdIn);
    close("stdout", stdOut);
    close("stderr", stdErr);
    close("stdin to container", stdInToContainer);
    close("stdout from container", stdOutFromContainer);
    close("stderr from container", stdErrFromContainer);
    close("client", client);
    finished.countDown();
    onCompletion.run();
  }

  private void close(String name, Closeable closeable) {
    try {
      closeable.close();
      LOGGER.debug("{} closed {}", this, name);
    } catch (IOException e) {
      String message = String.format("%s error while closing %s", this, name);
      LOGGER.warn(message, e);
    }
  }

  /**
   * A listener for Docker to notify whenever the containerized process writes new output. The
   * listener copies each frame of the process's output to this DockerProcess's stdout or stderr.
   */
  private class OutputListener extends ResultCallback.Adapter<Frame> {
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
        String message = String.format("%s %s error while writing to %s",
            DockerProcess.this.toString(), this, streamType);
        LOGGER.error(message, e);
      }
    }
    @Override
    public String toString() {
      return "output listener";
    }

  }

  /**
   * A listener for Docker to notify when the containerized process terminates. The listener records
   * the process's exit code, stops watching its streams, closes this {@code DockerProcess}'s output
   * streams, and removes the Docker container.
   */
  private class TerminationListener extends ResultCallback.Adapter<WaitResponse> {
    @Override
    public void onNext(WaitResponse response) {
      Integer statusCode = response.getStatusCode();
      LOGGER.debug("{} {} called: process exited with status code {}",
          DockerProcess.this, this, statusCode);
      exitCode.set(statusCode);
      try {
        outputListener.close();
        outputListener.awaitCompletion();
      } catch (Exception e) {
        String message = String.format("%s error while removing %s", DockerProcess.this, this);
        LOGGER.warn(message, e);
      } finally {
        finish();
      }
    }

    @Override
    public String toString() {
      return "termination listener";
    }
  }
}
