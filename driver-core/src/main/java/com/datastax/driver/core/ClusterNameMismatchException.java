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
package com.datastax.driver.core;

/**
 * Indicates that we've attempted to connect to a node which cluster name doesn't match that of the
 * other nodes known to the driver.
 */
class ClusterNameMismatchException extends Exception {

  private static final long serialVersionUID = 0;

  public final EndPoint endPoint;
  public final String expectedClusterName;
  public final String actualClusterName;

  public ClusterNameMismatchException(
      EndPoint endPoint, String actualClusterName, String expectedClusterName) {
    super(
        String.format(
            "[%s] Host %s reports cluster name '%s' that doesn't match our cluster name '%s'. This host will be ignored.",
            endPoint, endPoint, actualClusterName, expectedClusterName));
    this.endPoint = endPoint;
    this.expectedClusterName = expectedClusterName;
    this.actualClusterName = actualClusterName;
  }
}
