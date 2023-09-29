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

import java.util.Collection;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import static com.google.common.base.Predicates.*;

public class OptionalCodec<T> extends TypeCodec.MappingCodec<Optional<T>, T> {

    private final Predicate<T> isAbsent;

    @SuppressWarnings("unchecked")
    public OptionalCodec(TypeCodec<T> codec) {
        this(codec, (Predicate<T>) isAbsent());
    }

    private static <T> Predicate<T> isAbsent() {
        return or(isNull(), new Predicate<T>() {
            @Override
            public boolean apply(T input) {
                return (input instanceof Collection && ((Collection)input).isEmpty()) ||
                    (input instanceof Map && ((Map)input).isEmpty());
            }
        });
    }

    public OptionalCodec(TypeCodec<T> codec, Predicate<T> isAbsent) {
        super(codec, new TypeToken<Optional<T>>(){}.where(new TypeParameter<T>(){}, codec.getJavaType()));
        this.isAbsent = isAbsent;
    }

    @Override
    protected Optional<T> deserialize(T value) {
        return isAbsent(value) ? Optional.<T>absent() : Optional.of(value);
    }

    @Override
    protected T serialize(Optional<T> value) {
        return value.isPresent() ? value.get() : absentValue();
    }

    protected T absentValue() {
        return null;
    }

    protected boolean isAbsent(T value) {
        return isAbsent.apply(value);
    }


}
