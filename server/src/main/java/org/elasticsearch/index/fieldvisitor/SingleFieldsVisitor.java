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
package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.TypeFieldMapper;

import java.io.IOException;
import java.util.function.Function;

public class SingleFieldsVisitor extends FieldsVisitor {
    // TODO it'd be nice not to extend our (very big) FieldsVisitor and maybe make a common superclass?
    private String field;

    public SingleFieldsVisitor(String field) {
        super(false);
        this.field = field;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        if (fieldInfo.name.equals(field)) {
            return Status.YES;
        }
        return Status.NO;
    }

    public void reset(String field) {
        this.field = field;
        super.reset();
    }

    @Override
    public void postProcess(MapperService mapperService, LeafReaderContext context, int docId,
            Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup) {
        try {
            super.postProcess(mapperService, null, -1, null);
        } catch (IOException e) {
            // TODO a common superclass for field visiting would really help here
            throw new ElasticsearchException("Shouldn't be possible", e);
        }
        if (id != null) {
            addValue(IdFieldMapper.NAME, id);
        }
        if (type != null) {
            addValue(TypeFieldMapper.NAME, type);
        }
    }
}
