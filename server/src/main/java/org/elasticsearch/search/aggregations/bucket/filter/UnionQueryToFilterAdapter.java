package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.Bits;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class UnionQueryToFilterAdapter extends QueryToFilterAdapter<Query> {
    UnionQueryToFilterAdapter(IndexSearcher searcher, String key, Query query) {
        super(searcher, key, query);
    }

    @Override
    QueryToFilterAdapter<?> union(Query extraQuery) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    void collectOrRegisterUnion(LeafReaderContext ctx, CollectorSource collectorSource, Bits live) throws IOException {
        // NOCOMMIT unify with the places we get a BulkScorer
        Scorer scorer = weight().scorer(ctx);
        if (scorer == null) {
            // Nothing to collect
            return;
        }
        collectorSource.addToUnion(scorer);
    }

    public static void collectUnion(List<UnionFilter> filters, DocIdSetIterator topLevel, Bits live)
        throws IOException {
        if (filters.size() == 1) {
            collectUnionSingle(filters.get(0), topLevel, live);
        } else {
            collectUnionMany(filters, topLevel, live);
        }
        Releasables.close(filters.stream().map(u -> u.collector).collect(toList()));
    }

    /**
     * Collect a single filter without all of the complexity of {@link #collectUnionMany}.
     */
    private static void collectUnionSingle(UnionFilter filter, DocIdSetIterator topLevel, Bits live) throws IOException {
        filter.collector.setScorer(filter.scorer);
        DocIdSetIterator disi = ConjunctionDISI.intersectIterators(
            List.of(filter.scorer.iterator(), topLevel)
        );
        TwoPhaseIterator iter = TwoPhaseIterator.unwrap(disi);
        if (iter == null) {
            for (int doc = disi.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi.nextDoc()) {
                if (live == null || live.get(doc)) {
                    filter.collector.collect(doc);
                }
            }
        } else {
            for (int doc = iter.approximation().nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.approximation().nextDoc()) {
                if ((live == null || live.get(doc)) && iter.matches()) {
                    filter.collector.collect(doc);
                }
            }
        }
    }

    /**
     * Collect many filters in a single pass.
     * <p>
     * We manage this by building a custom {@link TwoPhaseIterator} that
     * matches the conjunction of all filters and remembers which filters
     * matched. When we hit a matching document we iterate the list of
     * matching filters and fire them into the collector.
     */
    private static void collectUnionMany(List<UnionFilter> filters, DocIdSetIterator topLevel, Bits live) throws IOException {
        TwoPhaseDisjunction filtersDisjunction = TwoPhaseDisjunction.build(filters);
        TwoPhaseIterator iter = TwoPhaseIterator.unwrap(ConjunctionDISI.intersectIterators(
            List.of(TwoPhaseIterator.asDocIdSetIterator(filtersDisjunction), topLevel)
        ));
        /*
         * TwoPhaseIterator.unwrap will return null if the iterator its
         * unwrapping doesn't have two phases. But for us it always will
         * because we're joining our two phase disjunction. As the name
         * implies, it is always run in two phases, even if it could be
         * run in one.
         */
        for (int doc = iter.approximation().nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iter.approximation().nextDoc()) {
            if ((live == null || live.get(doc)) && iter.matches()) {
                filtersDisjunction.collectMatches();
            }
        }
    }

    static class UnionFilter {
        private final Scorer scorer;
        private final ReleasableLeafCollector collector;

        public UnionFilter(Scorer scorer, ReleasableLeafCollector collector) {
            this.scorer = scorer;
            this.collector = collector;
        }
    }

    private static class TwoPhaseDisjunction extends TwoPhaseIterator {
        static TwoPhaseDisjunction build(List<UnionFilter> filters) throws IOException {
            DisiPriorityQueue queue = new DisiPriorityQueue(filters.size());
            for (UnionFilter f : filters) {
                f.collector.setScorer(f.scorer);
                queue.add(new FilterDisiWrapper(f.scorer, f.collector));
            }
            return new TwoPhaseDisjunction(queue);
        }

        private final DisiPriorityQueue queue;
        private DisiWrapper verifiedMatches;
        private DisiWrapper unverifiedMatches;

        public TwoPhaseDisjunction(DisiPriorityQueue queue) {
            super(new DisjunctionDISIApproximation(queue));
            this.queue = queue;
        }

        void collectMatches() throws IOException {
            while (verifiedMatches != null) {
                ((FilterDisiWrapper) verifiedMatches).collector.collect(verifiedMatches.doc);
                verifiedMatches = verifiedMatches.next;
            }
            while (unverifiedMatches != null) {
                if (unverifiedMatches.twoPhaseView.matches()) {
                    ((FilterDisiWrapper) verifiedMatches).collector.collect(verifiedMatches.doc);
                }
                unverifiedMatches = unverifiedMatches.next;
            }
        }

        @Override
        public boolean matches() throws IOException {
            verifiedMatches = null;
            unverifiedMatches = null;
            DisiWrapper w = queue.topList();
            while (w != null) {
                DisiWrapper next = w.next;

                if (w.twoPhaseView == null) {
                    w.next = verifiedMatches;
                    verifiedMatches = w;
                } else {
                    w.next = unverifiedMatches;
                    unverifiedMatches = w;
                }
                w = next;
            }

            if (verifiedMatches != null) {
                return true;
            }

            w = unverifiedMatches;
            while (w != null) {
                unverifiedMatches = w.next;

                if (w.twoPhaseView.matches()) {
                    w.next = null;
                    verifiedMatches = w;
                    return true;
                }
                w = unverifiedMatches;
            }

            return false;
        }

        @Override
        public float matchCost() {
            return 0; // NOCOMMIT pick a real cost
        }
    }

    private static class FilterDisiWrapper extends DisiWrapper {
        private final LeafCollector collector;

        public FilterDisiWrapper(Scorer scorer, LeafCollector collector) {
            super(scorer);
            this.collector = collector;
        }
    }
}
