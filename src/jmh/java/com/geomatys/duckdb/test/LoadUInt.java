package com.geomatys.duckdb.test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Thread)
public class LoadUInt {

    @Param({"1"})
    public int nValues;

    @Param({"allocate", "allocateDirect"})
    public String allocationMethod;

    private ByteBuffer buffer;
    private long expectedSum;

    @Setup(Level.Trial)
    public void setup() {
        var bufferSize = Integer.BYTES * nValues;
        buffer = allocationMethod == "allocateDirect"
                ? ByteBuffer.allocateDirect(bufferSize)
                : ByteBuffer.allocate(bufferSize);

        // Force little endian to match DuckDB behavior
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        final var rand = new Random();
        for (int i = 0; i < nValues; i++) {
            final int value = rand.nextInt();
            expectedSum += Integer.toUnsignedLong(value);
            buffer.putInt(value);
        }
        buffer.rewind();
    }

    @Benchmark
    public long loadUIntAsSuggested() {
        long sum = 0;
        buffer.rewind();
        for (int idx = 0; idx < nValues; idx++) {
            sum += Integer.toUnsignedLong(buffer.getInt());
        }
        if (sum != expectedSum) throw new AssertionError("Computed sum does not match expected");
        return sum;
    }

    @Benchmark
    public long loadUIntAsDuckDb() {
        long sum = 0;
        buffer.rewind();
        for (int idx = 0; idx < nValues; idx++) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            buffer.get(buf.array(), 0, Integer.BYTES);
            sum += buf.getLong();
        }
        if (sum != expectedSum) throw new AssertionError("Computed sum does not match expected");
        return sum;
    }
}
