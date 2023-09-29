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
package com.datastax.driver.mapping;

import com.google.common.reflect.TypeToken;

/**
 * The types of way to persist a JAVA Enum.
 */
public enum EnumType {
    /**
     * Persists enumeration values using their ordinal in the Enum declaration.
     * <p>
     * Note that this relies on the order of the values in source code, and will change
     * if values are added/removed before existing ones.
     */
    ORDINAL(Integer.class),
    /** Persists enumeration values using the string they have been declared with. */
    STRING(String.class);

    @SuppressWarnings("unchecked")
    EnumType(Class<?> klass) {
        this.pivotType = (TypeToken<Object>)TypeToken.of(klass);
    }

    /** The type enum constants will be turned into before being saved */
    final TypeToken<Object> pivotType;
}
