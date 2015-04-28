package com.gemstone.gemfire.rest.internal.web.http.converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.util.Assert;

/**
 * The CustomMappingJackson2HttpMessageConverter class...
 * 
 * @author John Blum
 * @see org.springframework.http.converter.json.MappingJackson2HttpMessageConverter
 * @since 0.0.1
 */
@SuppressWarnings("unused")
public class CustomMappingJackson2HttpMessageConverter extends
    MappingJackson2HttpMessageConverter {

  protected static final int INITIAL_BYTE_ARRAY_BUFFER_SIZE = 8192;

  @Override
  protected void writeInternal(final Object object,
      final HttpOutputMessage outputMessage) throws IOException,
      HttpMessageNotWritableException {
    HttpOutputMessageWrapper outputMessageWrapper = new BufferingHttpOutputMessageWrapper(
        outputMessage);
    super.writeInternal(object, outputMessageWrapper);
    outputMessageWrapper.flush();
  }

  protected static final class BufferingHttpOutputMessageWrapper implements
      HttpOutputMessageWrapper {

    private final ByteArrayOutputStream outputStream;

    private final HttpOutputMessage httpOutputMessage;

    protected BufferingHttpOutputMessageWrapper(
        final HttpOutputMessage httpOutputMessage) {
      Assert.notNull(httpOutputMessage,
          "The HttpOutputMessage instance to wrap must not be null!");
      this.httpOutputMessage = httpOutputMessage;
      this.outputStream = new ByteArrayOutputStream(
          INITIAL_BYTE_ARRAY_BUFFER_SIZE);
    }

    @Override
    public OutputStream getBody() throws IOException {
      return outputStream;
    }

    public long getContentLength() {
      return outputStream.size();
    }

    @Override
    public HttpHeaders getHeaders() {
      return httpOutputMessage.getHeaders();
    }

    public void flush() throws IOException {
      getHeaders().setContentLength(getContentLength());
      outputStream.writeTo(httpOutputMessage.getBody());
      outputStream.reset();
    }
  }

  /**
   * While sound idea in theory to "count the bytes as you stream/write", thus
   * preserving memory, this does not work in practice since the HTTP headers
   * must be written to the HTTP output stream response before the body!
   */
  protected static class ContentLengthAccessibleHttpOutputMessageWrapper implements
      HttpOutputMessageWrapper {

    private final ByteCountingOutputStream outputStream;

    private final HttpOutputMessage httpOutputMessage;

    protected ContentLengthAccessibleHttpOutputMessageWrapper(
        final HttpOutputMessage httpOutputMessage) throws IOException {
      Assert.notNull(httpOutputMessage,
          "The HttpOutputMessage instance to wrap must not be null!");
      this.httpOutputMessage = httpOutputMessage;
      this.outputStream = new ByteCountingOutputStream(
          this.httpOutputMessage.getBody());
    }

    @Override
    public OutputStream getBody() throws IOException {
      return outputStream;
    }

    public long getContentLength() {
      return outputStream.getByteCount();
    }

    @Override
    public HttpHeaders getHeaders() {
      return httpOutputMessage.getHeaders();
    }

    public void flush() throws IOException {
      getHeaders().setContentLength(getContentLength());
    }
  }

  protected interface HttpOutputMessageWrapper extends HttpOutputMessage {

    public long getContentLength();

    public void flush() throws IOException;

  }

  protected static final class ByteCountingOutputStream extends OutputStream {

    private AtomicLong byteCount = new AtomicLong(0l);

    private final OutputStream outputStream;

    protected ByteCountingOutputStream(final OutputStream outputStream) {
      Assert
          .notNull(outputStream, "The OutputStream to wrap must not be null!");
      this.outputStream = outputStream;
    }

    protected long getByteCount() {
      return byteCount.get();
    }

    @Override
    public void write(final int byteData) throws IOException {
      outputStream.write(byteData);
      byteCount.incrementAndGet();
    }
  }

}
