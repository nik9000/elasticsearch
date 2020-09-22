/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.GrokCaptureExtracter;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyMap;

public class BooleanGrokValues extends BooleanFieldScript { // NOCOMMIT this shouldn't extend script
    // NOCOMMIT we shouldn't really need a factory
    static BooleanFieldScript.Factory factory(
        String sourceField,
        Grok grok,
        Function<Consumer<Boolean>, GrokCaptureExtracter> buildExtracter
    ) {
        return (fieldName, params, searchLookup) -> {
            MappedFieldType sourceFieldType = searchLookup.doc().mapperService().fieldType(sourceField);
            if (sourceFieldType == null) {
                throw new IllegalArgumentException("source field [" + sourceField + "] not found");
            }
            IndexFieldData<?> ifd = searchLookup.doc().getForField(sourceFieldType);
            return ctx -> {
                SortedBinaryDocValues docValues = ifd.load(ctx).getBytesValues();
                // NOCOMMIT it is important that SortedBinaryDocValues are actually utf-8 characters. Anything else is likely to blow up.
                return new BooleanGrokValues(fieldName, searchLookup, ctx, grok, buildExtracter, docValues);
            };
        };
    }

    private final Grok grok;
    private final GrokCaptureExtracter extracter;
    private final SortedBinaryDocValues docValues;

    public BooleanGrokValues(
        String fieldName,
        SearchLookup searchLookup,
        LeafReaderContext ctx,
        Grok grok,
        Function<Consumer<Boolean>, GrokCaptureExtracter> buildExtracter,
        SortedBinaryDocValues docValues
    ) {
        super(fieldName, emptyMap(), searchLookup, ctx);
        this.grok = grok;
        extracter = buildExtracter.apply(this::emit);
        this.docValues = docValues;
    }

    @Override
    public void execute() {
        try {
            if (false == docValues.advanceExact(docId())) {  // NOCOMMIT remove copy and paste when we can clean up hierarchy
                return;
            }
            for (int i = 0; i < docValues.docValueCount(); i++) {
                BytesRef text = docValues.nextValue();
                grok.match(text.bytes, text.offset, text.length, extracter);
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // NOCOMMIT whatever this ends up extending it should throw IOException
        }
    }
}
