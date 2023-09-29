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
package com.datastax.driver.core.querybuilder;

import java.util.List;

public abstract class Clause extends Utils.Appendeable {

    protected final String name;

    private Clause(String name) {
        this.name = name;
    };

    String name() {
        return name;
    }

    abstract Object firstValue();

    static class SimpleClause extends Clause {

        private final String op;
        private final Object value;

        SimpleClause(String name, String op, Object value) {
            super(name);
            this.op = op;
            this.value = value;
        }

        @Override
        void appendTo(StringBuilder sb) {
            Utils.appendName(name, sb).append(op);
            Utils.appendValue(value, sb);
        }

        @Override
        Object firstValue() {
            return value;
        }
    }

    static class InClause extends Clause {

        private final List<Object> values;

        InClause(String name, List<Object> values) {
            super(name);
            this.values = values;

            if (values == null)
                throw new IllegalArgumentException("Missing values for IN clause");
        }

        @Override
        void appendTo(StringBuilder sb) {
            Utils.appendName(name, sb).append(" IN (");
            Utils.joinAndAppendValues(sb, ",", values).append(")");
        }

        @Override
        Object firstValue() {
            return values.isEmpty() ? null : values.get(0);
        }
    }
}
