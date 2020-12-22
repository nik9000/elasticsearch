package org.elasticsearch.index.fielddata.ordinals;

import org.elasticsearch.index.fielddata.ordinals.GlobalOrdToSegmentAndSegmentOrd.Reader;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdToSegmentAndSegmentOrd.ReaderProvider;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdToSegmentAndSegmentOrd.Writer;
import org.elasticsearch.index.fielddata.ordinals.OrdinalSequenceTests.DirectoryIO;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class GlobalOrdToSegmentAndSegmentOrdTests extends ESTestCase {
    public void testEmpty() throws IOException {
        try (Writer writer = writerThatNeverWrites()) {
            assertIdentity(writer.readerProvider());
        }
    }

    public void testAllInFirstSegment() throws IOException {
        try (Writer writer = writerThatNeverWrites()) {
            int count = randomInt(1000);
            for (int i = 0; i < count; i++) {
                writer.write(i, 0, i);
            }
            assertIdentity(writer.readerProvider());
        }
    }

    private void assertIdentity(ReaderProvider provider) throws IOException {
        try {
            assertThat(provider.ramBytesUsed(), lessThan(100L));
            Reader reader = provider.get();
            long l = randomLong();
            assertThat(reader.containingSegment(l), equalTo(0));
            assertThat(reader.containingSegmentOrd(l), equalTo(l));
        } finally {
            provider.close();
        }
    }

    public void testAllInOneSegment() throws IOException {
        int count = randomInt(1000);
        try (
            DirectoryIO segmentIO = new DirectoryIO();
            DirectoryIO segmentOrdIO = new DirectoryIO();
            Writer writer = new Writer(segmentIO, segmentOrdIO, count, count, count)
        ) {
            int[] expectedSegment = new int[count];
            long[] expectedSegmentOrd = new long[count];
            for (int i = 0; i < count; i++) {
                writer.write(i, i, 0);
                expectedSegment[i] = i;
                expectedSegmentOrd[i] = 0;
            }
            assertExpected(expectedSegment, expectedSegmentOrd, writer.readerProvider());
        }
    }

    public void testAllInFewSegments() throws IOException {
        for (int count = 10; count < 100_000_000; count *= 10) {
            for (int segmentCount : new int[] { 2, 10, 50, 100, 1000 }) {
                long[] segmentOrds = new long[segmentCount];

                int[] expectedSegment = new int[count];
                long[] expectedSegmentOrd = new long[count];
                long maxDelta = 0;
                for (int i = 0; i < count; i++) {
                    int segment = randomInt(segmentCount - 1);
                    expectedSegment[i] = segment;
                    expectedSegmentOrd[i] = segmentOrds[segment];
                    maxDelta = Math.max(maxDelta, i - segmentOrds[segment]);
                    segmentOrds[segment]++;
                }

                try (
                    DirectoryIO segmentIO = new DirectoryIO();
                    DirectoryIO segmentOrdIO = new DirectoryIO();
                    Writer writer = new Writer(segmentIO, segmentOrdIO, count, segmentCount, maxDelta)
                ) {
                    long start = System.nanoTime();
                    for (int i = 0; i < count; i++) {
                        writer.write(i, expectedSegment[i], expectedSegmentOrd[i]);
                    }
                    long time = System.nanoTime() - start;
                    ReaderProvider provider = writer.readerProvider();
                    System.err.printf(
                        Locale.ROOT,
                        "adsfdsaf count: %09d segments: %04d disk: %09d ram: %03d took: %010d\n",
                        count,
                        segmentCount,
                        provider.diskBytesUsed(),
                        provider.ramBytesUsed(),
                        time
                    );
                    assertExpected(expectedSegment, expectedSegmentOrd, provider);
                }
            }
        }
    }

    private void assertExpected(int[] expectedSegment, long[] expectedSegmentOrd, ReaderProvider provider) throws IOException {
        try {
            assertThat(expectedSegmentOrd.length, equalTo(expectedSegment.length));
            assertThat(provider.ramBytesUsed(), greaterThan(100L));
            assertThat(provider.ramBytesUsed(), lessThan(1000L));
            Reader reader = provider.get();
            for (int i = 0; i < expectedSegment.length; i++) {
                assertThat(reader.containingSegment(i), equalTo(expectedSegment[i]));
                assertThat(reader.containingSegmentOrd(i), equalTo(expectedSegmentOrd[i]));
            }
            // And again with random access
            for (int n = 0; n < 1000; n++) {
                int i = between(0, expectedSegment.length - 1);
                assertThat(reader.containingSegment(i), equalTo(expectedSegment[i]));
                assertThat(reader.containingSegmentOrd(i), equalTo(expectedSegmentOrd[i]));
            }
        } finally {
            provider.close();
        }
    }

    Writer writerThatNeverWrites() throws IOException {
        return new Writer(OrdinalSequenceTests.ioThatNeverWrites(), OrdinalSequenceTests.ioThatNeverWrites(), 0, 0, 0);
    }
}
