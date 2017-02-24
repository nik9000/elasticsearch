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

/**
 * Tests casts not interacting with operators.
 */
public class CastTests extends ScriptTestCase {
    /**
     * Currently these do not adopt the return value, we issue a separate cast!
     */
    public void testMethodCallDef() {
        assertEquals(5, exec("def x = 5; return (int)x.longValue();"));
    }

    public void testUnboxMethodParameters() {
        assertEquals('a', exec("'a'.charAt(Integer.valueOf(0))"));
    }

    /**
     * Test that without a cast, we fail when conversions would narrow.
     */
    public void testIllegalConversions() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("long x = 5L; int y = +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("long x = 5L; int y = (x + x); return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("boolean x = true; int y = +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("boolean x = true; int y = (x ^ false); return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("long x = 5L; boolean y = +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("long x = 5L; boolean y = (x + x); return y");
        });
    }

    /**
     * Test that even with a cast, some things aren't allowed.
     */
    public void testIllegalExplicitConversions() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("boolean x = true; int y = (int) +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("boolean x = true; int y = (int) (x ^ false); return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("long x = 5L; boolean y = (boolean) +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("long x = 5L; boolean y = (boolean) (x + x); return y");
        });
    }

    /**
     * Test that without a cast, we fail when conversions would narrow.
     */
    public void testIllegalConversionsDef() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = 5L; int y = +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = 5L; int y = (x + x); return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = true; int y = +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = true; int y = (x ^ false); return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = 5L; boolean y = +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = 5L; boolean y = (x + x); return y");
        });
    }

    public void testIllegalCastInMethodArgument() {
        assertEquals('a', exec("'a'.charAt(0)"));
        Exception e = expectScriptThrows(ClassCastException.class, () -> exec("'a'.charAt(0L)"));
        assertEquals("Cannot cast from [long] to [int].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () -> exec("'a'.charAt(0.0f)"));
        assertEquals("Cannot cast from [float] to [int].", e.getMessage());
        e = expectScriptThrows(ClassCastException.class, () -> exec("'a'.charAt(0.0d)"));
        assertEquals("Cannot cast from [double] to [int].", e.getMessage());
    }

    /**
     * Test that even with a cast, some things aren't allowed.
     * (stuff that methodhandles explicitCastArguments would otherwise allow)
     */
    public void testIllegalExplicitConversionsDef() {
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = true; int y = (int) +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = true; int y = (int) (x ^ false); return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = 5L; boolean y = (boolean) +x; return y");
        });
        expectScriptThrows(ClassCastException.class, () -> {
            exec("def x = 5L; boolean y = (boolean) (x + x); return y");
        });
    }

    public void testWideningCastOK() {
        assertEquals(5, exec("Integer i = Integer.valueOf(5); Number n = i; n"));
    }

    public void testNarrowingCastRequiresExplicit() {
        assertEquals(5, exec("Number n = Integer.valueOf(5); Integer i = (Integer) n; i"));
        Exception e = expectScriptThrows(ClassCastException.class, () -> exec("Number n = Integer.valueOf(5); Integer i = n; i"));
        assertEquals("Cannot cast from [Number] to [Integer].", e.getMessage());
    }

    public void testDefToNumericExplicitCasts() {
        assertEquals(true, exec("     def d = true;       (boolean) d"));
        assertEquals((byte) 5, exec(" def d = 5;          (byte) d"));
        assertEquals((short) 5, exec("def d = 5;          (short) d"));
        assertEquals('c', exec("      def d = (char) 'c'; (char) d"));
        assertEquals(5, exec("        def d = 5;          (int) d"));
        assertEquals(5L, exec("       def d = 5;          (long) d"));
        assertEquals(5.0f, exec("     def d = 5;          (float) d"));
        assertEquals(5.0d, exec("     def d = 5;          (double) d"));
    }

    public void testDefToNumericImplicitCasts() {
        assertEquals(true, exec("     def d = true;      boolean b = d; b"));
        assertEquals((byte) 5, exec(" def d = (byte) 5;  byte b = d;    b"));
        assertEquals((short) 5, exec("def d = (short) 5; short s = d;   s"));
        assertEquals('c', exec("def d = (char) 'c';      char c = d;    c"));
        assertEquals(5, exec("        def d = 5;         int i = d;     i"));
        assertEquals(5, exec("        def d = (short)5;  int i = d;     i"));
        assertEquals(5L, exec("       def d = 5L;        long l = d;    l"));
        assertEquals(5L, exec("       def d = 5;         long l = d;    l"));
        assertEquals(5.0f, exec("     def d = 5f;        float f = d;   f"));
        assertEquals(5.0f, exec("     def d = 5;         float f = d;   f"));
        assertEquals(5.0f, exec("     def d = 5L;        float f = d;   f"));
        assertEquals(5.0d, exec("     def d = 5d;        double n = d;  n"));
        assertEquals(5.0d, exec("     def d = 5;         double n = d;  n"));
        assertEquals(5.0d, exec("     def d = 5L;        double n = d;  n"));

        expectScriptThrows(ClassCastException.class, () -> exec("def d = 5;    byte b = d;   b"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 5;    short s = d;  s"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 5L;   int i = d;    i"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 5.0f; long l = d;   l"));
        expectScriptThrows(ClassCastException.class, () -> exec("def d = 5.0d; float f = d;  f"));
    }

    public void testDefToOtherCasts() {
        assertEquals("test", exec("def d = new StringBuilder('test'); (StringBuilder) d").toString());
        assertEquals("test", exec("def d = new StringBuilder('test'); StringBuilder sb = d; sb").toString());
    }
}
