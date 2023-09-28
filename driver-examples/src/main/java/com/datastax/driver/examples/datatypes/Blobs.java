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
package com.datastax.driver.examples.datatypes;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.Bytes;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * Inserts and retrieves values in BLOB columns.
 *
 * <p>By default, the Java Driver maps this type to {@link java.nio.ByteBuffer}. The ByteBuffer API
 * is a bit tricky to use at times, so we will show common pitfalls as well. We strongly recommend
 * that you read the {@link java.nio.Buffer} and {@link ByteBuffer} API docs and become familiar
 * with the capacity, limit and position properties. <a
 * href="http://tutorials.jenkov.com/java-nio/buffers.html">This tutorial</a> might also help.
 *
 * <p>Preconditions: - a Cassandra cluster is running and accessible through the contacts points
 * identified by CONTACT_POINTS and PORT; - FILE references an existing file.
 *
 * <p>Side effects: - creates a new keyspace "examples" in the cluster. If a keyspace with this name
 * already exists, it will be reused; - creates a table "examples.blobs". If it already exists, it
 * will be reused; - inserts data in the table.
 */
public class Blobs {

  static String[] CONTACT_POINTS = {"127.0.0.1"};
  static int PORT = 9042;

  static File FILE = new File(Blobs.class.getResource("/cassandra_logo.png").getFile());

  public static void main(String[] args) throws IOException {
    Cluster cluster = null;
    try {
      cluster = Cluster.builder().addContactPoints(CONTACT_POINTS).withPort(PORT).build();
      Session session = cluster.connect();

      createSchema(session);
      allocateAndInsert(session);
      retrieveSimpleColumn(session);
      retrieveMapColumn(session);
      insertConcurrent(session);
      insertFromAndRetrieveToFile(session);
    } finally {
      if (cluster != null) cluster.close();
    }
  }

  private static void createSchema(Session session) {
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS examples "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    session.execute(
        "CREATE TABLE IF NOT EXISTS examples.blobs(k int PRIMARY KEY, b blob, m map<text, blob>)");
  }

  private static void allocateAndInsert(Session session) {
    // One way to get a byte buffer is to allocate it and fill it yourself:
    ByteBuffer buffer = ByteBuffer.allocate(16);
    while (buffer.hasRemaining()) buffer.put((byte) 0xFF);

    // Don't forget to flip! The driver expects a buffer that is ready for reading. That is, it will
    // consider all
    // the data between buffer.position() and buffer.limit().
    // Right now we are positioned at the end because we just finished writing, so if we passed the
    // buffer as-is it
    // would appear to be empty:
    assert buffer.limit() - buffer.position() == 0;

    buffer.flip();
    // Now position is back to the beginning, so the driver will see all 16 bytes.
    assert buffer.limit() - buffer.position() == 16;

    Map<String, ByteBuffer> map = new HashMap<String, ByteBuffer>();
    map.put("test", buffer);
    session.execute("INSERT INTO examples.blobs (k, b, m) VALUES (1, ?, ?)", buffer, map);
  }

  private static void retrieveSimpleColumn(Session session) {
    Row row = session.execute("SELECT b, m FROM examples.blobs WHERE k = 1").one();

    ByteBuffer buffer = row.getBytes("b");

    // The driver always returns buffers that are ready for reading.
    assert buffer.limit() - buffer.position() == 16;

    // One way to read from the buffer is to use absolute getters. Do NOT start reading at index 0,
    // as the buffer
    // might start at a different position (we'll see an example of that later).
    for (int i = buffer.position(); i < buffer.limit(); i++) {
      byte b = buffer.get(i);
      assert b == (byte) 0xFF;
    }

    // Another way is to use relative getters.
    while (buffer.hasRemaining()) {
      byte b = buffer.get();
      assert b == (byte) 0xFF;
    }
    // Note that relative getters change the position, so when we're done reading we're at the end
    // again.
    assert buffer.position() == buffer.limit();

    // Reset the position for the next operation.
    buffer.flip();

    // Yet another way is to convert the buffer to a byte array. Do NOT use buffer.array(), because
    // it returns the
    // buffer's *backing array*, which is not the same thing as its contents:
    // - not all byte buffers have backing arrays
    // - even then, the backing array might be larger than the buffer's contents
    //
    // The driver provides a utility method that handles those details for you:
    byte[] array = Bytes.getArray(buffer);
    assert array.length == 16;
    for (byte b : array) {
      assert b == (byte) 0xFF;
    }
  }

  private static void retrieveMapColumn(Session session) {
    Row row = session.execute("SELECT b, m FROM examples.blobs WHERE k = 1").one();

    // The map columns illustrates the pitfalls with position() and array().
    Map<String, ByteBuffer> m = row.getMap("m", String.class, ByteBuffer.class);
    ByteBuffer buffer = m.get("test");

    // We did get back a buffer that contains 16 bytes as expected.
    assert buffer.limit() - buffer.position() == 16;
    // However, it is not positioned at 0. And you can also see that its backing array contains more
    // than 16 bytes.
    // What happens is that the buffer is a "view" of the last 16 of a 32-byte array.
    // This is an implementation detail and you shouldn't have to worry about it if you process the
    // buffer correctly
    // (don't iterate from 0, use Bytes.getArray()).
    assert buffer.position() == 16;
    assert buffer.array().length == 32;
  }

  private static void insertConcurrent(Session session) {
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO examples.blobs (k, b) VALUES (1, :b)");

    // This is another convenient utility provided by the driver. It's useful for tests.
    ByteBuffer buffer = Bytes.fromHexString("0xffffff");

    // When you pass a byte buffer to a bound statement, it creates a shallow copy internally with
    // the
    // buffer.duplicate() method.
    BoundStatement boundStatement = preparedStatement.bind();
    boundStatement.setBytes("b", buffer);

    // This means you can now move in the original buffer, without affecting the insertion if it
    // happens later.
    buffer.position(buffer.limit());

    session.execute(boundStatement);
    Row row = session.execute("SELECT b FROM examples.blobs WHERE k = 1").one();
    assert Bytes.toHexString(row.getBytes("b")).equals("0xffffff");

    buffer.flip();

    // HOWEVER duplicate() only performs a shallow copy. The two buffers still share the same
    // contents. So if you
    // modify the contents of the original buffer, this will affect another execution of the bound
    // statement.
    buffer.put(0, (byte) 0xaa);
    session.execute(boundStatement);
    row = session.execute("SELECT b FROM examples.blobs WHERE k = 1").one();
    assert Bytes.toHexString(row.getBytes("b")).equals("0xaaffff");

    // This will also happen if you use the async API, e.g. create the bound statement, call
    // executeAsync() on it
    // and reuse the buffer immediately.

    // If you reuse buffers concurrently and want to avoid those issues, perform a deep copy of the
    // buffer before
    // passing it to the bound statement.
    int startPosition = buffer.position();
    ByteBuffer buffer2 = ByteBuffer.allocate(buffer.limit() - startPosition);
    buffer2.put(buffer);
    buffer.position(startPosition);
    buffer2.flip();
    boundStatement.setBytes("b", buffer2);
    session.execute(boundStatement);

    // Note: unlike BoundStatement, SimpleStatement does not duplicate its arguments, so even the
    // position will be
    // affected if you change it before executing the statement. Again, resort to deep copies if
    // required.
  }

  private static void insertFromAndRetrieveToFile(Session session) throws IOException {
    ByteBuffer buffer = readAll(FILE);
    session.execute("INSERT INTO examples.blobs (k, b) VALUES (1, ?)", buffer);

    File tmpFile = File.createTempFile("blob", ".png");
    System.out.printf("Writing retrieved buffer to %s%n", tmpFile.getAbsoluteFile());

    Row row = session.execute("SELECT b FROM examples.blobs WHERE k = 1").one();
    writeAll(row.getBytes("b"), tmpFile);
  }

  // Note:
  // - this is written with Java 6 APIs; if you're on a more recent version this can be improved
  // (try-with-resources,
  //   new-new io...)
  // - this reads the whole file in memory in one go. If your file does not fit in memory you should
  // probably not
  //   insert it into Cassandra either ;)
  private static ByteBuffer readAll(File file) throws IOException {
    FileInputStream inputStream = null;
    boolean threw = false;
    try {
      inputStream = new FileInputStream(file);
      FileChannel channel = inputStream.getChannel();
      ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
      channel.read(buffer);
      buffer.flip();
      return buffer;
    } catch (IOException e) {
      threw = true;
      throw e;
    } finally {
      close(inputStream, threw);
    }
  }

  private static void writeAll(ByteBuffer buffer, File file) throws IOException {
    FileOutputStream outputStream = null;
    boolean threw = false;
    try {
      outputStream = new FileOutputStream(file);
      FileChannel channel = outputStream.getChannel();
      channel.write(buffer);
    } catch (IOException e) {
      threw = true;
      throw e;
    } finally {
      close(outputStream, threw);
    }
  }

  private static void close(Closeable inputStream, boolean threw) throws IOException {
    if (inputStream != null)
      try {
        inputStream.close();
      } catch (IOException e) {
        if (!threw) throw e; // else preserve original exception
      }
  }
}
