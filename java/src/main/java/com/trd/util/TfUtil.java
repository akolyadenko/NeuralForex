package com.trd.util;

import org.apache.commons.codec.digest.PureJavaCrc32C;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.Checksum;

public class TfUtil {
    public static void write(DataOutput out, byte[] record) {
        write(out, record, 0, record.length);
    }

    public static void write(DataOutput output, byte[] record, int offset, int length) {
        try {
            byte[] len = toInt64LE(length);
            output.write(len);
            output.write(toInt32LE(Crc32C.maskedCrc32c(len)));
            output.write(record, offset, length);
            output.write(toInt32LE(Crc32C.maskedCrc32c(record, offset, length)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] toInt64LE(long data) {
        byte[] buff = new byte[8];
        ByteBuffer bb = ByteBuffer.wrap(buff);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.putLong(data);
        return buff;
    }

    private static byte[] toInt32LE(int data) {
        byte[] buff = new byte[4];
        ByteBuffer bb = ByteBuffer.wrap(buff);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(data);
        return buff;
    }

    public static class Crc32C implements Checksum {
        private static final int MASK_DELTA = 0xa282ead8;
        private PureJavaCrc32C crc32C;

        public static int maskedCrc32c(byte[] data) {
            return maskedCrc32c(data, 0, data.length);
        }

        public static int maskedCrc32c(byte[] data, int offset, int length) {
            Crc32C crc32c = new Crc32C();
            crc32c.update(data, offset, length);
            return crc32c.getMaskedValue();
        }

        /**
         * Return a masked representation of crc.
         * <p>
         *  Motivation: it is problematic to compute the CRC of a string that
         *  contains embedded CRCs.  Therefore we recommend that CRCs stored
         *  somewhere (e.g., in files) should be masked before being stored.
         * </p>
         * @param crc CRC
         * @return masked CRC
         */
        public static int mask(int crc) {
            // Rotate right by 15 bits and add a constant.
            return ((crc >>> 15) | (crc << 17)) + MASK_DELTA;
        }

        /**
         * Return the crc whose masked representation is masked_crc.
         * @param maskedCrc masked CRC
         * @return crc whose masked representation is masked_crc
         */
        public static int unmask(int maskedCrc) {
            int rot = maskedCrc - MASK_DELTA;
            return ((rot >>> 17) | (rot << 15));
        }

        public Crc32C() {
            crc32C = new PureJavaCrc32C();
        }

        public int getMaskedValue() {
            return mask(getIntValue());
        }

        public int getIntValue() {
            return (int) getValue();
        }

        @Override public void update(int b) {
            crc32C.update(b);
        }

        @Override public void update(byte[] b, int off, int len) {
            crc32C.update(b, off, len);
        }

        @Override public long getValue() {
            return crc32C.getValue();
        }

        @Override public void reset() {
            crc32C.reset();
        }
    }
}
