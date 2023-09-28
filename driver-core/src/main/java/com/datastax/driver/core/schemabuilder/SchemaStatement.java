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
package com.datastax.driver.core.schemabuilder;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.google.common.base.Strings;
import java.nio.ByteBuffer;
import java.util.Map;

/** A DDL statement generated by {@link SchemaBuilder}. */
public abstract class SchemaStatement extends RegularStatement {

  static final String STATEMENT_START = "\n\t";
  static final String COLUMN_FORMATTING = "\n\t\t";

  private volatile String cache;

  abstract String buildInternal();

  @Override
  public String getQueryString(CodecRegistry codecRegistry) {
    if (cache == null) {
      cache = buildInternal();
    }
    return cache;
  }

  @Override
  public ByteBuffer[] getValues(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    // DDL statements never have values
    return new ByteBuffer[0];
  }

  @Override
  public boolean hasValues(CodecRegistry codecRegistry) {
    return false;
  }

  @Override
  public Map<String, ByteBuffer> getNamedValues(
      ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    // DDL statements never have values
    return null;
  }

  @Override
  public boolean usesNamedValues() {
    return false;
  }

  @Override
  public String getKeyspace() {
    // This is exposed for token-aware routing. Since there is no token awareness for DDL
    // statements, we don't need to
    // return anything here (even if a keyspace has been explicitly set in the statement).
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @param protocolVersion unused by this implementation (the key is always null for schema
   *     statements).
   * @param codecRegistry unused by this implementation (the key is always null for schema
   *     statements).
   */
  @Override
  public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    return null; // there is no token awareness for DDL statements
  }

  static void validateNotEmpty(String columnName, String label) {
    if (Strings.isNullOrEmpty(columnName)) {
      throw new IllegalArgumentException(label + " should not be null or blank");
    }
  }

  static void validateNotNull(Object value, String label) {
    if (value == null) {
      throw new IllegalArgumentException(label + " should not be null");
    }
  }

  static void validateNotKeyWord(String label, String message) {
    if (Metadata.isReservedCqlKeyword(label)) {
      throw new IllegalArgumentException(message);
    }
  }

  static SchemaStatement fromQueryString(final String queryString) {
    return new SchemaStatement() {
      @Override
      public String buildInternal() {
        return queryString;
      }
    };
  }

  StatementStart asStatementStart() {
    return new StatementStart() {
      @Override
      public String buildInternal() {
        return SchemaStatement.this.buildInternal();
      }
    };
  }
}
