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
package org.elasticsearch.common.regex;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class RegexTests extends ESTestCase {
    public void testFlags() {
        String[] supportedFlags = new String[]{"CASE_INSENSITIVE", "MULTILINE", "DOTALL", "UNICODE_CASE", "CANON_EQ", "UNIX_LINES",
                "LITERAL", "COMMENTS", "UNICODE_CHAR_CLASS", "UNICODE_CHARACTER_CLASS"};
        int[] flags = new int[]{Pattern.CASE_INSENSITIVE, Pattern.MULTILINE, Pattern.DOTALL, Pattern.UNICODE_CASE, Pattern.CANON_EQ,
                Pattern.UNIX_LINES, Pattern.LITERAL, Pattern.COMMENTS, Regex.UNICODE_CHARACTER_CLASS};
        Random random = random();
        int num = 10 + random.nextInt(100);
        for (int i = 0; i < num; i++) {
            int numFlags = random.nextInt(flags.length + 1);
            int current = 0;
            StringBuilder builder = new StringBuilder();
            for (int j = 0; j < numFlags; j++) {
                int index = random.nextInt(flags.length);
                current |= flags[index];
                builder.append(supportedFlags[index]);
                if (j < numFlags - 1) {
                    builder.append("|");
                }
            }
            String flagsToString = Regex.flagsToString(current);
            assertThat(Regex.flagsFromString(builder.toString()), equalTo(current));
            assertThat(Regex.flagsFromString(builder.toString()), equalTo(Regex.flagsFromString(flagsToString)));
            Pattern.compile("\\w\\d{1,2}", current); // accepts the flags?
        }
    }

    public void testDoubleWildcardMatch() {
        assertTrue(Regex.simpleMatch("ddd", "ddd"));
        assertTrue(Regex.simpleMatch("d*d*d", "dadd"));
        assertTrue(Regex.simpleMatch("**ddd", "dddd"));
        assertFalse(Regex.simpleMatch("**ddd", "fff"));
        assertTrue(Regex.simpleMatch("fff*ddd", "fffabcddd"));
        assertTrue(Regex.simpleMatch("fff**ddd", "fffabcddd"));
        assertFalse(Regex.simpleMatch("fff**ddd", "fffabcdd"));
        assertTrue(Regex.simpleMatch("fff*******ddd", "fffabcddd"));
        assertFalse(Regex.simpleMatch("fff******ddd", "fffabcdd"));
    }

    public void testBuildWhitelist() {
        assertMatchesTooMuch(singletonList("*"));
        assertMatchesTooMuch(singletonList("**"));
        assertMatchesTooMuch(singletonList("***"));
        assertMatchesTooMuch(Arrays.asList("realstuff", "*"));
        assertMatchesTooMuch(Arrays.asList("*", "realstuff"));

        String random = randomAsciiOfLength(5);

        CharacterRunAutomaton whitelist = Regex.buildWhitelist(emptyList(), "");
        assertFalse(whitelist.run(""));
        assertFalse("Shouldn't match " + random, whitelist.run(random));

        whitelist = Regex.buildWhitelist(singletonList(random), "");
        assertFalse(whitelist.run(""));
        assertTrue("Should match " + random, whitelist.run(random));
        assertFalse("Shouldn't match " + random + "AAA", whitelist.run(random + "AAA"));

        whitelist = Regex.buildWhitelist(singletonList(random + "*"), "");
        assertFalse(whitelist.run(""));
        assertTrue("Should match " + random, whitelist.run(random));
        assertTrue("Should match " + random + "AAA", whitelist.run(random + "AAA"));
    }

    private void assertMatchesTooMuch(List<String> whitelist) {
        String extra = "extra" + randomAsciiOfLength(5);
        Exception e = expectThrows(IllegalArgumentException.class, () -> Regex.buildWhitelist(whitelist, extra));
        assertEquals("Refusing to start because whitelist " + whitelist + " accepts all addresses. " + extra, e.getMessage());
    }

}
