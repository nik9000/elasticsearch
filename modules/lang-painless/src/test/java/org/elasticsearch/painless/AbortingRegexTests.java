/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

import org.elasticsearch.test.ESTestCase;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AbortingRegexTests extends ESTestCase {
    public void testAlreadyStopped() {
        AbortingCharSequence c = new AbortingCharSequence("foo") {
            @Override
            protected void check() {
                throw new IllegalStateException();
            }
        };
        Matcher m = Pattern.compile("foo").matcher(c);
        expectThrows(IllegalStateException.class, () -> m.matches());
    }

    public void testAfterSome() {
        class AfterSome extends AbortingCharSequence {
            private int remaining = 0;

            public AfterSome(CharSequence delegate, int max) {
                super(delegate);
                remaining = max;
            }

            @Override
            protected void check() {
                if (--remaining <= 0) {
                    throw new IllegalStateException();
                }
            }
        };
        class Counter extends AbortingCharSequence {
            private int used = 0;

            public Counter(CharSequence delegate) {
                super(delegate);
            }

            @Override
            protected void check() {
                used++;
                if (used % 1000000 == 0) {
                    System.err.println(used);
                }
            }
        };
        {
            Matcher m = Pattern.compile("foo").matcher(new AfterSome("foo", 10));
            assertTrue(m.matches());
        }
        {
            Matcher m = Pattern.compile("foooooooooo").matcher(new AfterSome("foooooooooo", 10));
            expectThrows(IllegalStateException.class, () -> m.matches());
        }
        {
            Matcher m = Pattern.compile("f(a|b|c|d|e|o)+").matcher(new AfterSome("foo", 10));
            expectThrows(IllegalStateException.class, () -> m.matches());
        }
        {
            Matcher m = Pattern.compile("f(a|b|c|d|e|o){10,88}").matcher(new AfterSome("fooooooooooooooooooooooooooooooooo", 80));
            expectThrows(IllegalStateException.class, () -> m.matches());
        }
//        {
//            Counter c = new Counter("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
//            Matcher m = Pattern.compile("((x+x+x+){3,100}x+){3,100}x+").matcher(c);
//            assertTrue(m.matches());
//            System.err.println(c.used);
//        }
        {
            Matcher m = Pattern.compile("((x+x+x+){3,100}x+){3,100}x+") // Suffer
                    .matcher(new AfterSome("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", 100000000));
            expectThrows(IllegalStateException.class, () -> m.matches());
        }
    }

}
