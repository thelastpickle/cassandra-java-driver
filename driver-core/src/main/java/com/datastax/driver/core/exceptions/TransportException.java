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
package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.EndPoint;

/**
 * A connection exception that has to do with the transport itself, i.e. that suggests the node is
 * down.
 */
public class TransportException extends ConnectionException {

  private static final long serialVersionUID = 0;

  public TransportException(EndPoint endPoint, String msg, Throwable cause) {
    super(endPoint, msg, cause);
  }

  public TransportException(EndPoint endPoint, String msg) {
    super(endPoint, msg);
  }

  @Override
  public TransportException copy() {
    return new TransportException(getEndPoint(), getRawMessage(), this);
  }
}
