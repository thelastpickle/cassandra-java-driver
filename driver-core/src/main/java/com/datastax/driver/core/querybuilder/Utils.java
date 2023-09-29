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

import com.datastax.driver.core.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

// Static utilities private to the query builder
abstract class Utils {

    private static final Pattern cnamePattern = Pattern.compile("\\w+(?:\\[.+\\])?");

    static StringBuilder joinAndAppend(StringBuilder sb, String separator, List<? extends Appendeable> values, List<Object> variables) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            values.get(i).appendTo(sb, variables);
        }
        return sb;
    }

    static StringBuilder joinAndAppendNames(StringBuilder sb, String separator, List<?> values) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            appendName(values.get(i), sb);
        }
        return sb;
    }

    static StringBuilder joinAndAppendValues(StringBuilder sb, String separator, List<?> values, List<Object> variables) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0)
                sb.append(separator);
            appendValue(values.get(i), sb, variables);
        }
        return sb;
    }

    // Returns false if it's not really serializable (function call, bind markers, ...)
    static boolean isSerializable(Object value) {
        if (value instanceof BindMarker || value instanceof FCall || value instanceof CName)
            return false;

        if (value instanceof RawString)
            return false;

        // We also don't serialize fixed size number types. The reason is that if we do it, we will
        // force a particular size (4 bytes for ints, ...) and for the query builder, we don't want
        // users to have to bother with that.
        if (value instanceof Number && !(value instanceof BigInteger || value instanceof BigDecimal))
            return false;

        return true;
    }

    static ByteBuffer[] convert(List<Object> values) {
        ByteBuffer[] serializedValues = new ByteBuffer[values.size()];
        for (int i = 0; i < values.size(); i++) {
            try {
                serializedValues[i] = DataType.serializeValue(values.get(i));
            } catch (IllegalArgumentException e) {
                // Catch and rethrow to provide a more helpful error message (one that include which value is bad)
                throw new IllegalArgumentException(String.format("Value %d of type %s does not correspond to any CQL3 type", i, values.get(i).getClass()));
            }
        }
        return serializedValues;
    }

    static StringBuilder appendValue(Object value, StringBuilder sb, List<Object> variables) {
        if (variables == null || !isSerializable(value))
            return appendValue(value, sb, false);

        sb.append('?');
        variables.add(value);
        return sb;
    }

    static StringBuilder appendFlatValue(Object value, StringBuilder sb) {
        appendFlatValue(value, sb, false);
        return sb;
    }

    private static StringBuilder appendValue(Object value, StringBuilder sb, boolean rawValue) {
        // That is kind of lame but lacking a better solution
        if (appendValueIfLiteral(value, sb))
            return sb;

        if (appendValueIfCollection(value, sb, rawValue))
            return sb;

        appendStringIfValid(value, sb, rawValue);
        return sb;
    }

    private static void appendFlatValue(Object value, StringBuilder sb, boolean rawValue) {
        if (appendValueIfLiteral(value, sb))
            return;

        appendStringIfValid(value, sb, rawValue);
    }

    private static void appendStringIfValid(Object value, StringBuilder sb, boolean rawValue) {
        if (value instanceof RawString) {
            sb.append(value.toString());
        } else {
            if (!(value instanceof String)) {
                String msg = String.format("Invalid value %s of type unknown to the query builder", value);
                if (value instanceof byte[])
                    msg += " (for blob values, make sure to use a ByteBuffer)";
                throw new IllegalArgumentException(msg);
            }

            if (rawValue)
                sb.append((String) value);
            else
                appendValueString((String) value, sb);
        }
    }

    private static boolean appendValueIfLiteral(Object value, StringBuilder sb) {
        if (value instanceof Number || value instanceof UUID || value instanceof Boolean) {
            sb.append(value);
            return true;
        } else if (value instanceof InetAddress) {
            sb.append(DataType.inet().format(value));
            return true;
        } else if (value instanceof Date) {
            sb.append(DataType.timestamp().format(value));
            return true;
        } else if (value instanceof ByteBuffer) {
            sb.append(DataType.blob().format(value));
            return true;
        } else if (value instanceof BindMarker) {
            sb.append(value);
            return true;
        } else if (value instanceof FCall) {
            FCall fcall = (FCall) value;
            sb.append(fcall.name).append('(');
            for (int i = 0; i < fcall.parameters.length; i++) {
                if (i > 0)
                    sb.append(',');
                appendValue(fcall.parameters[i], sb, null);
            }
            sb.append(')');
            return true;
        } else if (value instanceof CName) {
            appendName(((CName) value).name, sb);
            return true;
        } else if (value == null) {
            sb.append("null");
            return true;
        } else {
            return false;
        }
    }

    @SuppressWarnings("rawtypes")
    private static boolean appendValueIfCollection(Object value, StringBuilder sb, boolean rawValue) {
        if (value instanceof List) {
            appendList((List) value, sb, rawValue);
            return true;
        } else if (value instanceof Set) {
            appendSet((Set) value, sb, rawValue);
            return true;
        } else if (value instanceof Map) {
            appendMap((Map) value, sb, rawValue);
            return true;
        } else {
            return false;
        }
    }

    static StringBuilder appendCollection(Object value, StringBuilder sb, List<Object> variables) {
        if (variables == null || !isSerializable(value)) {
            boolean wasCollection = appendValueIfCollection(value, sb, false);
            assert wasCollection;
        } else {
            sb.append('?');
            variables.add(value);
        }
        return sb;
    }

    static StringBuilder appendList(List<?> l, StringBuilder sb, boolean rawValue) {
        sb.append('[');
        for (int i = 0; i < l.size(); i++) {
            if (i > 0)
                sb.append(',');
            appendFlatValue(l.get(i), sb, rawValue);
        }
        sb.append(']');
        return sb;
    }

    static StringBuilder appendSet(Set<?> s, StringBuilder sb, boolean rawValue) {
        sb.append('{');
        boolean first = true;
        for (Object elt : s) {
            if (first) first = false;
            else sb.append(',');
            appendFlatValue(elt, sb, rawValue);
        }
        sb.append('}');
        return sb;
    }

    static StringBuilder appendMap(Map<?, ?> m, StringBuilder sb, boolean rawValue) {
        sb.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> entry : m.entrySet()) {
            if (first)
                first = false;
            else
                sb.append(',');
            appendFlatValue(entry.getKey(), sb, rawValue);
            sb.append(':');
            appendFlatValue(entry.getValue(), sb, rawValue);
        }
        sb.append('}');
        return sb;
    }

    static boolean containsBindMarker(Object value) {
        if (value == QueryBuilder.bindMarker())
            return true;

        if (!(value instanceof FCall))
            return false;

        FCall fcall = (FCall) value;
        for (Object param : fcall.parameters)
            if (containsBindMarker(param))
                return true;
        return false;
    }

    static boolean isIdempotent(Object value) {
        if (value == null) {
            return true;
        } else if (value instanceof Assignment) {
            Assignment assignment = (Assignment) value;
            return assignment.isIdempotent();
        } else if (value instanceof FCall) {
            return false;
        } else if (value instanceof RawString) {
            return false;
        } else if (value instanceof Collection) {
            for (Object elt : ((Collection) value)) {
                if (!isIdempotent(elt))
                    return false;
            }
            return true;
        } else if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!isIdempotent(entry.getKey()) || !isIdempotent(entry.getValue()))
                    return false;
            }
        }
        return true;
    }

    private static StringBuilder appendValueString(String value, StringBuilder sb) {
        return sb.append(DataType.text().format(value));
    }

    static boolean isRawValue(Object value) {
        return value != null
                && !(value instanceof FCall)
                && !(value instanceof CName)
                && !(value instanceof BindMarker);
    }

    static String toRawString(Object value) {
        return appendValue(value, new StringBuilder(), true).toString();
    }

    static StringBuilder appendName(String name, StringBuilder sb) {
        name = name.trim();
        // FIXME: checking for token( specifically is uber ugly, we'll need some better solution.
        if (cnamePattern.matcher(name).matches() || name.startsWith("\"") || name.startsWith("token("))
            sb.append(name);
        else
            sb.append('"').append(name).append('"');
        return sb;
    }

    static StringBuilder appendName(Object name, StringBuilder sb) {
        if (name instanceof String) {
            appendName((String) name, sb);
        } else if (name instanceof CName) {
            appendName(((CName) name).name, sb);
        } else if (name instanceof FCall) {
            FCall fcall = (FCall) name;
            sb.append(fcall.name).append('(');
            for (int i = 0; i < fcall.parameters.length; i++) {
                if (i > 0)
                    sb.append(',');
                appendValue(fcall.parameters[i], sb, null);
            }
            sb.append(')');
        } else if (name instanceof Alias) {
            Alias alias = (Alias) name;
            appendName(alias.column, sb);
            sb.append(" AS ").append(alias.alias);
        } else {
            throw new IllegalArgumentException(String.format("Invalid column %s of type unknown of the query builder", name));
        }
        return sb;
    }

    static abstract class Appendeable {
        abstract void appendTo(StringBuilder sb, List<Object> values);

        abstract boolean containsBindMarker();
    }

    // Simple method to replace a single character. String.replace is a bit too
    // inefficient (see JAVA-67)
    static String replace(String text, char search, String replacement) {
        if (text == null || text.isEmpty())
            return text;

        int nbMatch = 0;
        int start = -1;
        do {
            start = text.indexOf(search, start + 1);
            if (start != -1)
                ++nbMatch;
        } while (start != -1);

        if (nbMatch == 0)
            return text;

        int newLength = text.length() + nbMatch * (replacement.length() - 1);
        char[] result = new char[newLength];
        int newIdx = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == search) {
                for (int r = 0; r < replacement.length(); r++)
                    result[newIdx++] = replacement.charAt(r);
            } else {
                result[newIdx++] = c;
            }
        }
        return new String(result);
    }

    static class RawString {
        private final String str;

        RawString(String str) {
            this.str = str;
        }

        @Override
        public String toString() {
            return str;
        }
    }

    static class FCall {

        private final String name;
        private final Object[] parameters;

        FCall(String name, Object... parameters) {
            checkNotNull(name);
            this.name = name;
            this.parameters = parameters;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(name).append('(');
            for (int i = 0; i < parameters.length; i++) {
                if (i > 0)
                    sb.append(',');
                sb.append(parameters[i]);
            }
            sb.append(')');
            return sb.toString();
        }

    }

    static class CName {
        private final String name;

        CName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    static class Alias {
        private final Object column;
        private final String alias;

        Alias(Object column, String alias) {
            this.column = column;
            this.alias = alias;
        }

        @Override
        public String toString() {
            return String.format("%s AS %s", column, alias);
        }
    }
}
