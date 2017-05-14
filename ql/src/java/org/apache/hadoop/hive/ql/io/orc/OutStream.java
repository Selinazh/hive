/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

class OutStream extends PositionedOutputStream {

  interface OutputReceiver {
    /**
     * Output the given buffer to the final destination
     * @param buffer the buffer to output
     * @throws IOException
     */
    void output(ByteBuffer buffer) throws IOException;
  }

  static final int HEADER_SIZE = 3;
  private final String name;
  private final OutputReceiver receiver;
  // if enabled the stream will be suppressed when writing stripe
  private boolean suppress;

  /**
   * Stores the uncompressed bytes that have been serialized, but not
   * compressed yet. When this fills, we compress the entire buffer.
   * the buffer capacity is guaranteed not large than bufferSize
   */
  private ChannelBuffer current = null;

  /**
   * Stores the compressed bytes until we have a full buffer and then outputs
   * them to the receiver. If no compression is being done, this buffer
   * will always be null and the current buffer will be sent directly to the
   * receiver.
   */
  private ChannelBuffer compressed = null;

  private final int bufferSize;
  private final CompressionCodec codec;
  private long compressedBytes = 0;
  private long uncompressedBytes = 0;

  OutStream(String name,
            int bufferSize,
            CompressionCodec codec,
            OutputReceiver receiver) throws IOException {
    this.name = name;
    this.bufferSize = bufferSize;
    this.codec = codec;
    this.receiver = receiver;
    this.suppress = false;
  }

  public void clear() throws IOException {
    flush();
    suppress = false;
  }

  /**
   * Write the length of the compressed bytes. Life is much easier if the
   * header is constant length, so just use 3 bytes. Considering most of the
   * codecs want between 32k (snappy) and 256k (lzo, zlib), 3 bytes should
   * be plenty. We also use the low bit for whether it is the original or
   * compressed bytes.
   * @param buffer the buffer to write the header to
   * @param position the position in the buffer to write at
   * @param val the size in the file
   * @param original is it uncompressed
   */
  private static void writeHeader(ChannelBuffer buffer,
                                  int position,
                                  int val,
                                  boolean original) {
    buffer.setByte(position, (byte) ((val << 1) + (original ? 1 : 0)));
    buffer.setByte(position + 1, (byte) (val >> 7));
    buffer.setByte(position + 2, (byte) (val >> 15));
  }

  /**
   * Throws exception if the bufferSize argument equals or exceeds 2^(3*8 - 1).
   * See {@link OutStream#writeHeader(ChannelBuffer, int, int, boolean)}.
   * The bufferSize needs to be expressible in 3 bytes, and uses the least significant byte
   * to indicate original/compressed bytes.
   * @param bufferSize The ORC compression buffer size being checked.
   * @throws IllegalArgumentException If bufferSize value exceeds threshold.
   */
  static void assertBufferSizeValid(int bufferSize) throws IllegalArgumentException {
    if (bufferSize >= (1 << 23)) {
      throw new IllegalArgumentException("Illegal value of ORC compression buffer size: " + bufferSize);
    }
  }

  private void getNewInputBuffer() throws IOException {
    current = ChannelBuffers.dynamicBuffer();
    if (codec != null) {
      writeHeader(current, 0, bufferSize, true);
      current.writerIndex(HEADER_SIZE); // write after 
    }
  }

  /**
   * Allocate a new output buffer if we are compressing.
   */
  private ChannelBuffer getNewOutputBuffer() throws IOException {
    return ChannelBuffers.dynamicBuffer();
  }

  private int getHeaderSize() {
    return codec == null ? 0 : HEADER_SIZE;
  }

  private int getBufferLimit() {
    return codec == null ? bufferSize : bufferSize + HEADER_SIZE;
  }

  private void flip() throws IOException {
    current.readerIndex(getHeaderSize());
  }

  @Override
  public void write(int i) throws IOException {
    if (current == null) {
      getNewInputBuffer();
    }
    if (current.readableBytes() == getBufferLimit()) {
      spill();
    }
    uncompressedBytes += 1;
    current.writeByte((byte) i);
  }

  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    if (current == null) {
      getNewInputBuffer();
    }
    int limit = getBufferLimit();
    int remaining = Math.min(limit - current.readableBytes(), length);
    current.writeBytes(bytes, offset, remaining);
    uncompressedBytes += remaining;
    length -= remaining;
    while (length != 0) {
      spill();
      offset += remaining;
      remaining = Math.min(limit - current.readableBytes(), length);
      current.writeBytes(bytes, offset, remaining);
      uncompressedBytes += remaining;
      length -= remaining;
    }
  }

  private void spill() throws java.io.IOException {
    // if there isn't anything in the current buffer, don't spill
    if (current == null ||
        current.writerIndex() == (getHeaderSize())) {
      return;
    }
    flip();
    if (codec == null) {
      receiver.output(current.toByteBuffer());
      getNewInputBuffer();
    } else {
      if (compressed == null) {
        compressed = getNewOutputBuffer();
      } 
      int sizePosn = compressed.writerIndex();
      compressed.writerIndex(compressed.writerIndex() + HEADER_SIZE);
      if (codec.compress(current.toByteBuffer(), compressed)) {
        uncompressedBytes = 0;
        getNewInputBuffer();
        // find the total bytes in the chunk
        int totalBytes = compressed.writerIndex() - sizePosn - HEADER_SIZE;
       
        compressedBytes += totalBytes + HEADER_SIZE;
        writeHeader(compressed, sizePosn, totalBytes, false);
        // if we have less than the next header left, spill it.
        if (bufferSize - compressed.readableBytes() < HEADER_SIZE) {
          compressed.readerIndex(0);
          receiver.output(compressed.toByteBuffer());
          compressed = getNewOutputBuffer();
        } else {
          // grow buffer if necessary because current capacity may not reach bufferSize yet
          compressed.ensureWritableBytes(HEADER_SIZE);
        }
      } else {
        compressedBytes += uncompressedBytes + HEADER_SIZE;
        uncompressedBytes = 0;
        // we are using the original, but need to spill the current
        // compressed buffer first. So back up to where we started,
        // flip it and add it to done.
        if (sizePosn != 0) {
          compressed.writerIndex(sizePosn);
          compressed.readerIndex(0);
          receiver.output(compressed.toByteBuffer());
        } 
        compressed = getNewOutputBuffer();

        // now add the current buffer into the done list and get a new one.
        // update the header with the current length
        writeHeader(current, 0, current.readableBytes(), true);
        current.readerIndex(0);
        receiver.output(current.toByteBuffer());
        getNewInputBuffer();
      }
    }
  }

  void getPosition(PositionRecorder recorder) throws IOException {
    if (codec == null) {
      recorder.addPosition(uncompressedBytes);
    } else {
      recorder.addPosition(compressedBytes);
      recorder.addPosition(uncompressedBytes);
    }
  }

  @Override
  public void flush() throws IOException {
    spill();
    if (compressed != null && compressed.readable()) {
      compressed.readerIndex(0);
      receiver.output(compressed.toByteBuffer());
      compressed = null;
    }
    uncompressedBytes = 0;
    compressedBytes = 0;
    current = null;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public long getBufferSize() {
    long result = 0;
    if (current != null) {
      result += current.capacity();
    }
    if (compressed != null) {
      result += compressed.capacity();
    }
    return result;
  }

  /**
   * Set suppress flag
   */
  public void suppress() {
    suppress = true;
  }

  /**
   * Returns the state of suppress flag
   * @return value of suppress flag
   */
  public boolean isSuppressed() {
    return suppress;
  }
}

