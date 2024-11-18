/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

public class CategorizeRawBlockHash extends AbstractCategorizeBlockHash {
    private final CategorizeEvaluator evaluator;

    public CategorizeRawBlockHash(int channel, BlockFactory blockFactory, boolean outputPartial) {
        super(blockFactory, channel, outputPartial);
        CategorizationAnalyzer analyzer = new CategorizationAnalyzer(
            // TODO: should be the same analyzer as used in Production
            new CustomAnalyzer(
                TokenizerFactory.newFactory("whitespace", WhitespaceTokenizer::new),
                new CharFilterFactory[0],
                new TokenFilterFactory[0]
            ),
            true
        );
        this.evaluator = new CategorizeEvaluator(analyzer, categorizer, blockFactory);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        try (IntBlock result = (IntBlock) evaluator.eval(page.getBlock(channel()))) {
            addInput.add(0, result);
        }
    }

    @Override
    public void close() {
        evaluator.close();
    }

    /**
     * NOCOMMIT: Super-duper copy-pasted from the actually generated evaluator; needs cleanup.
     */
    public static final class CategorizeEvaluator implements Releasable {
        private final CategorizationAnalyzer analyzer;

        private final TokenListCategorizer.CloseableTokenListCategorizer categorizer;

        private final BlockFactory blockFactory;

        static int process(
            BytesRef v,
            @Fixed(includeInToString = false, build = true) CategorizationAnalyzer analyzer,
            @Fixed(includeInToString = false, build = true) TokenListCategorizer.CloseableTokenListCategorizer categorizer
        ) {
            return categorizer.computeCategory(v.utf8ToString(), analyzer).getId();
        }

        public CategorizeEvaluator(
            CategorizationAnalyzer analyzer,
            TokenListCategorizer.CloseableTokenListCategorizer categorizer,
            BlockFactory blockFactory
        ) {
            this.analyzer = analyzer;
            this.categorizer = categorizer;
            this.blockFactory = blockFactory;
        }

        public Block eval(BytesRefBlock vBlock) {
            BytesRefVector vVector = vBlock.asVector();
            if (vVector == null) {
                return eval(vBlock.getPositionCount(), vBlock);
            }
            IntVector vector = eval(vBlock.getPositionCount(), vVector);
            return vector.asBlock();
        }

        public IntBlock eval(int positionCount, BytesRefBlock vBlock) {
            try (IntBlock.Builder result = blockFactory.newIntBlockBuilder(positionCount)) {
                BytesRef vScratch = new BytesRef();
                for (int p = 0; p < positionCount; p++) {
                    if (vBlock.isNull(p)) {
                        result.appendNull();
                        continue;
                    }
                    int first = vBlock.getFirstValueIndex(p);
                    int count = vBlock.getValueCount(p);
                    if (count == 1) {
                        result.appendInt(process(vBlock.getBytesRef(first, vScratch), this.analyzer, this.categorizer));
                        continue;
                    }
                    int end = first + count;
                    for (int i = first; i < end; i++) {
                        result.appendInt(process(vBlock.getBytesRef(i, vScratch), this.analyzer, this.categorizer));
                    }
                }
                return result.build();
            }
        }

        public IntVector eval(int positionCount, BytesRefVector vVector) {
            try (IntVector.FixedBuilder result = blockFactory.newIntVectorFixedBuilder(positionCount)) {
                BytesRef vScratch = new BytesRef();
                for (int p = 0; p < positionCount; p++) {
                    result.appendInt(p, process(vVector.getBytesRef(p, vScratch), this.analyzer, this.categorizer));
                }
                return result.build();
            }
        }

        @Override
        public String toString() {
            return "CategorizeEvaluator";
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(analyzer, categorizer);
        }
    }
}
