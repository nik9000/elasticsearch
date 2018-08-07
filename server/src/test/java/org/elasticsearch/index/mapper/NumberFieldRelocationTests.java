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

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;

import java.io.IOException;

public class NumberFieldRelocationTests extends AbstractFieldRelocationTestCase {
    private NumberFieldMapper.NumberType numberType = randomFrom(NumberFieldMapper.NumberType.values());

    @Override
    protected String fieldType() {
        return numberType.typeName();
    }

    @Override
    protected void writeRandomValue(XContentBuilder builder) throws IOException {
        Number n = randomValidNumber();
        builder.value(usually() ? n : n.toString());
    }

    private Number randomValidNumber() {
        switch (numberType) {
        case HALF_FLOAT:
            return randomDoubleBetween(-65504, 65504, true);
        case FLOAT:
            return randomDoubleBetween(Float.MIN_VALUE, Float.MAX_VALUE, true);
        case DOUBLE:
            return randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
        case BYTE:
            return randomBoolean() ?
                    randomDoubleBetween(Byte.MIN_VALUE, Byte.MAX_VALUE, true) : between(Byte.MIN_VALUE, Byte.MAX_VALUE);
        case SHORT:
            return randomBoolean() ?
                    randomDoubleBetween(Short.MIN_VALUE, Short.MAX_VALUE, true) : between(Short.MIN_VALUE, Short.MAX_VALUE);
        case INTEGER:
            return randomBoolean() ?
                    randomDoubleBetween(Integer.MIN_VALUE, Integer.MAX_VALUE, true) : between(Integer.MIN_VALUE, Integer.MAX_VALUE);
        case LONG:
            return randomBoolean() ?
                    randomDoubleBetween(Long.MIN_VALUE, Long.MAX_VALUE, true) : randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
        default:
            throw new UnsupportedOperationException("unknown type " + numberType);
        }
    }
}
