/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.iceberg.deletionvectors;

import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RoaringBitmap32;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;

/** doc. */
public class IcebergPositionDeleteIndex {
    private static final int LENGTH_SIZE_BYTES = 4;
    private static final int MAGIC_NUMBER_SIZE_BYTES = 4;
    private static final int CRC_SIZE_BYTES = 4;
    private static final int BITMAP_DATA_OFFSET = 4;
    private static final int MAGIC_NUMBER = 1681511377;

    private static final long BITMAP_COUNT_SIZE_BYTES = 8L;
    private static final long BITMAP_KEY_SIZE_BYTES = 4L;

    // iceberg use bitmaps array to support positive 64-bit positions, while paimon only
    // supports 32-bit positions. So we can assume paimon's bitmap is an bitmap array with one
    // element in iceberg
    private static final int BITMAP_COUNT = 1;
    private static final int BITMAP_KEY = 0;

    private final RoaringBitmap32 roaringBitmap32;

    public IcebergPositionDeleteIndex(RoaringBitmap32 roaringBitmap) {
        this.roaringBitmap32 = roaringBitmap;
    }

    public long getCardinality() {
        return roaringBitmap32.getCardinality();
    }

    // TODO: add test,IcebergPositionDeleteIndex.serialize() equals to
    // BitmapPositionDeleteIndex.serialize()
    // serialize DeletionVector to ByteBuffer which can be read by iceberg
    public ByteBuffer serialize() {
        roaringBitmap32.get().runOptimize();
        int bitmapDataLength = computeBitmapDataLength(roaringBitmap32);
        byte[] bytes = new byte[LENGTH_SIZE_BYTES + bitmapDataLength + CRC_SIZE_BYTES];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putInt(bitmapDataLength);
        serializeBitmapData(bytes, bitmapDataLength, roaringBitmap32);
        int crcOffset = LENGTH_SIZE_BYTES + bitmapDataLength;
        int crc = computeChecksum(bytes, bitmapDataLength);
        buffer.putInt(crcOffset, crc);
        buffer.rewind();
        return buffer;
    }

    // serializes the bitmap data (magic bytes + bitmap) using the little-endian byte order
    private static void serializeBitmapData(
            byte[] bytes, int bitmapDataLength, RoaringBitmap32 bitmap) {
        // byteBuffer for serialized bitmap, using little-endian byte order
        ByteBuffer bitmapData = ByteBuffer.wrap(bytes, BITMAP_DATA_OFFSET, bitmapDataLength);
        bitmapData.order(ByteOrder.LITTLE_ENDIAN);

        bitmapData.putInt(MAGIC_NUMBER);

        validateByteOrder(bitmapData);
        // the length of bitmap array
        bitmapData.putLong(BITMAP_COUNT);
        // the key of the only bitmap element in array
        bitmapData.putInt(BITMAP_KEY);
        bitmap.get().serialize(bitmapData);
    }

    private int computeBitmapDataLength(RoaringBitmap32 bitmap) {
        long length =
                MAGIC_NUMBER_SIZE_BYTES
                        + BITMAP_COUNT_SIZE_BYTES
                        + BITMAP_KEY_SIZE_BYTES
                        + bitmap.get().serializedSizeInBytes();
        long bufferSize = LENGTH_SIZE_BYTES + length + CRC_SIZE_BYTES;
        Preconditions.checkArgument(bufferSize <= Integer.MAX_VALUE, "Can't serialize index > 2GB");
        return (int) length;
    }

    private static void validateByteOrder(ByteBuffer buffer) {
        Preconditions.checkArgument(
                buffer.order() == ByteOrder.LITTLE_ENDIAN,
                "Roaring bitmap serialization requires little-endian byte order");
    }

    // generates a 32-bit unsigned checksum for the magic bytes and serialized bitmap
    private static int computeChecksum(byte[] bytes, int bitmapDataLength) {
        CRC32 crc = new CRC32();
        crc.update(bytes, BITMAP_DATA_OFFSET, bitmapDataLength);
        return (int) crc.getValue();
    }
}
