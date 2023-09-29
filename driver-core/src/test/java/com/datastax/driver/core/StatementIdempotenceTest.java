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

import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;

public class StatementIdempotenceTest {
    @Test(groups = "unit")
    public void should_default_to_false_when_not_set_on_statement_nor_query_options() {
        QueryOptions queryOptions = new QueryOptions();
        SimpleStatement statement = new SimpleStatement("");

        assertThat(statement.isIdempotentWithDefault(queryOptions)).isFalse();
    }

    @Test(groups = "unit")
    public void should_use_query_options_when_not_set_on_statement() {
        QueryOptions queryOptions = new QueryOptions();
        SimpleStatement statement = new SimpleStatement("");

        for (boolean valueInOptions : new boolean[]{true, false}) {
            queryOptions.setDefaultIdempotence(valueInOptions);
            assertThat(statement.isIdempotentWithDefault(queryOptions)).isEqualTo(valueInOptions);
        }
    }

    @Test(groups = "unit")
    public void should_use_statement_when_set_on_statement() {
        QueryOptions queryOptions = new QueryOptions();
        SimpleStatement statement = new SimpleStatement("");

        for (boolean valueInOptions : new boolean[]{true, false})
            for (boolean valueInStatement : new boolean[]{true, false}) {
                queryOptions.setDefaultIdempotence(valueInOptions);
                statement.setIdempotent(valueInStatement);
                assertThat(statement.isIdempotentWithDefault(queryOptions)).isEqualTo(valueInStatement);
            }
    }

    @Test(groups = "unit")
    public void should_infer_for_built_statement() {
        for (BuiltStatement statement : idempotentBuiltStatements())
            assertThat(statement.isIdempotent())
                    .as(statement.getQueryString())
                    .isTrue();

        for (BuiltStatement statement : nonIdempotentBuiltStatements())
            assertThat(statement.isIdempotent())
                    .as(statement.getQueryString())
                    .isFalse();
    }

    @Test(groups = "unit")
    public void should_override_inferred_value_when_manually_set_on_built_statement() {
        for (boolean manualValue : new boolean[]{true, false}) {
            for (BuiltStatement statement : idempotentBuiltStatements()) {
                statement.setIdempotent(manualValue);
                assertThat(statement.isIdempotent()).isEqualTo(manualValue);
            }

            for (BuiltStatement statement : nonIdempotentBuiltStatements()) {
                statement.setIdempotent(manualValue);
                assertThat(statement.isIdempotent()).isEqualTo(manualValue);
            }
        }
    }

    private static ImmutableList<BuiltStatement> idempotentBuiltStatements() {
        return ImmutableList.<BuiltStatement>of(
                update("foo").with(set("v", 1)).where(eq("k", 1)), // set simple value
                update("foo").with(add("s", 1)).where(eq("k", 1)), // add to set
                update("foo").with(put("m", "a", 1)).where(eq("k", 1)), // put in map

                // select statements should be idempotent even with function calls
                select().countAll().from("foo").where(eq("k", 1)),
                select().ttl("v").from("foo").where(eq("k", 1)),
                select().writeTime("v").from("foo").where(eq("k", 1)),
                select().fcall("token", "k").from("foo").where(eq("k", 1))

        );
    }

    private static ImmutableList<BuiltStatement> nonIdempotentBuiltStatements() {
        return ImmutableList.of(
                update("foo").with(append("l", 1)).where(eq("k", 1)), // append to list
                update("foo").with(set("v", 1)).and(prepend("l", 1)).where(eq("k", 1)), // prepend to list
                update("foo").with(incr("c")).where(eq("k", 1)), // counter update

                // function calls

                insertInto("foo").value("k", 1).value("v", fcall("token", "k")),
                insertInto("foo").value("k", 1).value("v", now()),
                insertInto("foo").value("k", 1).value("v", uuid()),

                insertInto("foo").value("k", 1).value("v", Sets.newHashSet(fcall("token", "k"))),
                insertInto("foo").value("k", 1).value("v", Sets.newHashSet(now())),
                insertInto("foo").value("k", 1).value("v", Sets.newHashSet(uuid())),

                insertInto("foo").values(new String[]{"k", "v"}, new Object[]{1, fcall("token", "k")}),
                insertInto("foo").values(new String[]{"k", "v"}, new Object[]{1, now()}),
                insertInto("foo").values(new String[]{"k", "v"}, new Object[]{1, uuid()}),

                insertInto("foo").values(new String[]{"k", "v"}, new Object[]{1, ImmutableMap.of("foo", fcall("token", "k"))}),
                insertInto("foo").values(new String[]{"k", "v"}, new Object[]{1, ImmutableMap.of("foo", now())}),
                insertInto("foo").values(new String[]{"k", "v"}, new Object[]{1, ImmutableMap.of("foo", uuid())}),

                insertInto("foo").values(new String[]{"k", "v"}, new Object[]{1, ImmutableMap.of(fcall("token", "k"), "foo")}),
                insertInto("foo").values(new String[]{"k", "v"}, new Object[]{1, ImmutableMap.of(now(), "foo")}),
                insertInto("foo").values(new String[]{"k", "v"}, new Object[]{1, ImmutableMap.of(uuid(), "foo")}),

                update("foo").with(set("v", fcall("token", "k"))).where(eq("k", 1)),
                update("foo").with(set("v", now())).where(eq("k", 1)),
                update("foo").with(set("v", uuid())).where(eq("k", 1)),

                update("foo").with(set("v", Lists.newArrayList(fcall("token", "k")))).where(eq("k", 1)),
                update("foo").with(set("v", Lists.newArrayList(now()))).where(eq("k", 1)),
                update("foo").with(set("v", Lists.newArrayList(uuid()))).where(eq("k", 1)),

                delete().from("foo").where(lt("k", fcall("now"))),
                delete().from("foo").where(lt("k", now())),
                update("foo").where(eq("k", fcall("now"))),
                delete().listElt("a", 1).from("test_coll"),

                // LWT
                update("foo").where(eq("is", "charlie?")).ifExists(),
                update("foo").where(eq("good", "drivers")).onlyIf(contains("developers", "datastax")),
                update("foo").onlyIf().and(contains("developers", "datastax")).where(eq("good", "drivers")),
                update("foo").onlyIf(contains("developers", "datastax")).with(set("v", 0)),
                update("foo").with(set("v", 0)).onlyIf(contains("hello", "world")),

                insertInto("foo").value("k", 1).value("v", Sets.newHashSet(now())).ifNotExists(),

                delete().from("foo").where(eq("k", 2)).ifExists(),
                delete().from("foo").onlyIf(eq("k", 2)),

                // raw() calls

                insertInto("foo").value("k", 1).value("v", raw("foo()")),
                insertInto("foo").value("k", 1).value("v", Sets.newHashSet(raw("foo()"))),

                insertInto("foo").values(new String[]{"k", "v"}, new Object[]{1, raw("foo()")}),
                insertInto("foo").values(new String[]{"k", "v"}, new Object[]{1, ImmutableMap.of("foo", raw("foo()"))}),
                insertInto("foo").values(new String[]{"k", "v"}, new Object[]{1, ImmutableMap.of(raw("foo()"), "foo")}),

                update("foo").with(set("v", raw("foo()"))).where(eq("k", 1)),
                update("foo").with(set("v", Lists.newArrayList(raw("foo()")))).where(eq("k", 1))

        );
    }
}
