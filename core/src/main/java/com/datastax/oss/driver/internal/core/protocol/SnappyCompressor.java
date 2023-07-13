/*
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
package com.datastax.oss.driver.internal.core.protocol;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.util.DependencyCheck;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;
import org.xerial.snappy.Snappy;

@ThreadSafe
public class SnappyCompressor extends ByteBufCompressor {

  public SnappyCompressor(@SuppressWarnings("unused") DriverContext context) {
    if (!DependencyCheck.SNAPPY.isPresent()) {
      throw new IllegalStateException(
          "Could not find the Snappy library on the classpath "
              + "(the driver declares it as an optional dependency, "
              + "so you need to declare it explicitly)");
    }
  }

  @Override
  public String algorithm() {
    return "snappy";
  }

  @Override
  protected ByteBuf compressDirect(ByteBuf input) {
    int maxCompressedLength = Snappy.maxCompressedLength(input.readableBytes());
    // If the input is direct we will allocate a direct output buffer as well as this will allow us
    // to use Snappy.compress(ByteBuffer, ByteBuffer) and so eliminate memory copies.
    ByteBuf output = input.alloc().directBuffer(maxCompressedLength);
    try {
      ByteBuffer in = inputNioBuffer(input);
      // Increase reader index.
      input.readerIndex(input.writerIndex());

      ByteBuffer out = outputNioBuffer(output);
      int written = Snappy.compress(in, out);
      // Set the writer index so the amount of written bytes is reflected
      output.writerIndex(output.writerIndex() + written);
      return output;
    } catch (IOException e) {
      // release output buffer so we not leak and rethrow exception.
      output.release();
      throw new RuntimeException(e);
    }
  }

  @Override
  protected ByteBuf compressHeap(ByteBuf input) {
    int maxCompressedLength = Snappy.maxCompressedLength(input.readableBytes());
    int inOffset = input.arrayOffset() + input.readerIndex();
    byte[] in = input.array();
    int len = input.readableBytes();
    // Increase reader index.
    input.readerIndex(input.writerIndex());

    // Allocate a heap buffer from the ByteBufAllocator as we may use a PooledByteBufAllocator and
    // so can eliminate the overhead of allocate a new byte[].
    ByteBuf output = input.alloc().heapBuffer(maxCompressedLength);
    try {
      // Calculate the correct offset.
      int offset = output.arrayOffset() + output.writerIndex();
      byte[] out = output.array();
      int written = Snappy.compress(in, inOffset, len, out, offset);

      // Increase the writerIndex with the written bytes.
      output.writerIndex(output.writerIndex() + written);
      return output;
    } catch (IOException e) {
      // release output buffer so we not leak and rethrow exception.
      output.release();
      throw new RuntimeException(e);
    }
  }

  @Override
  protected ByteBuf decompressDirect(ByteBuf input) {
    ByteBuffer in = inputNioBuffer(input);
    // Increase reader index.
    input.readerIndex(input.writerIndex());

    ByteBuf output = null;
    try {
      if (!Snappy.isValidCompressedBuffer(in)) {
        throw new IllegalArgumentException(
            "Provided frame does not appear to be Snappy compressed");
      }
      // If the input is direct we will allocate a direct output buffer as well as this will allow
      // us to use Snappy.compress(ByteBuffer, ByteBuffer) and so eliminate memory copies.
      output = input.alloc().directBuffer(Snappy.uncompressedLength(in));
      ByteBuffer out = outputNioBuffer(output);

      int size = Snappy.uncompress(in, out);
      // Set the writer index so the amount of written bytes is reflected
      output.writerIndex(output.writerIndex() + size);
      return output;
    } catch (IOException e) {
      // release output buffer so we not leak and rethrow exception.
      if (output != null) {
        output.release();
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  protected ByteBuf decompressHeap(ByteBuf input) throws RuntimeException {
    // Not a direct buffer so use byte arrays...
    int inOffset = input.arrayOffset() + input.readerIndex();
    byte[] in = input.array();
    int len = input.readableBytes();
    // Increase reader index.
    input.readerIndex(input.writerIndex());

    ByteBuf output = null;
    try {
      if (!Snappy.isValidCompressedBuffer(in, inOffset, len)) {
        throw new IllegalArgumentException(
            "Provided frame does not appear to be Snappy compressed");
      }
      // Allocate a heap buffer from the ByteBufAllocator as we may use a PooledByteBufAllocator and
      // so can eliminate the overhead of allocate a new byte[].
      output = input.alloc().heapBuffer(Snappy.uncompressedLength(in, inOffset, len));
      // Calculate the correct offset.
      int offset = output.arrayOffset() + output.writerIndex();
      byte[] out = output.array();
      int written = Snappy.uncompress(in, inOffset, len, out, offset);

      // Increase the writerIndex with the written bytes.
      output.writerIndex(output.writerIndex() + written);
      return output;
    } catch (IOException e) {
      // release output buffer so we not leak and rethrow exception.
      if (output != null) {
        output.release();
      }
      throw new RuntimeException(e);
    }
  }
}
