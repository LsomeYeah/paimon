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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** doc. */
public class IcebergPuffinWriter implements Closeable {
    private static final byte[] MAGIC = new byte[] {0x50, 0x46, 0x41, 0x31};
    static final int FOOTER_STRUCT_FLAGS_LENGTH = 4;

    private final PositionOutputStream outputStream;
    private final List<IcebergBlobMetadata> writtenBlobsMetadata;
    private final Map<String, String> properties = Maps.newLinkedHashMap();

    private boolean headerWritten;
    private boolean finished;
    private Optional<Integer> footerSize = Optional.empty();
    private Optional<Long> fileSize = Optional.empty();

    public IcebergPuffinWriter(FileIO fileIO, Path path) throws IOException {
        this.outputStream = fileIO.newOutputStream(path, true);
        this.writtenBlobsMetadata = new ArrayList<>();
    }

    public IcebergBlobMetadata write(IcebergBlob blob) {
        Preconditions.checkNotNull(blob, "blob is null");
        checkNotFinished();
        try {
            writeHeaderIfNeeded();
            long fileOffset = outputStream.getPos();
            ByteBuffer rawData = blob.blobData().duplicate();
            int length = rawData.remaining();
            // write blob
            outputStream.write(rawData.array());
            IcebergBlobMetadata blobMetadata =
                    new IcebergBlobMetadata(
                            blob.type(),
                            blob.inputFields(),
                            blob.snapshotId(),
                            blob.sequenceNumber(),
                            fileOffset,
                            length,
                            null,
                            blob.properties());
            writtenBlobsMetadata.add(blobMetadata);
            return blobMetadata;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (!finished) {
            finish();
        }

        outputStream.close();
    }

    public void finish() throws IOException {
        checkNotFinished();
        writeHeaderIfNeeded();
        Preconditions.checkState(!footerSize.isPresent(), "footerSize already set");
        long footerOffset = outputStream.getPos();
        writeFooter();
        this.footerSize = Optional.of(Math.toIntExact(outputStream.getPos() - footerOffset));
        this.fileSize = Optional.of(outputStream.getPos());
        this.finished = true;
    }

    private void writeHeaderIfNeeded() throws IOException {
        if (headerWritten) {
            return;
        }

        this.outputStream.write(MAGIC);
        this.headerWritten = true;
    }

    private void checkNotFinished() {
        Preconditions.checkState(!finished, "Writer already finished");
    }

    private void writeIntegerLittleEndian(OutputStream outputStream, int value) throws IOException {
        outputStream.write(0xFF & value);
        outputStream.write(0xFF & (value >> 8));
        outputStream.write(0xFF & (value >> 16));
        outputStream.write(0xFF & (value >> 24));
    }

    private void writeFooter() throws IOException {
        IcebergFileMetadata fileMetadata =
                new IcebergFileMetadata(writtenBlobsMetadata, properties);
        ByteBuffer footerJson =
                ByteBuffer.wrap(fileMetadata.toJson().getBytes(StandardCharsets.UTF_8));
        // Default, footerPayload is not compressed
        ByteBuffer footerPayload = footerJson.duplicate();
        outputStream.write(MAGIC);
        int footerPayloadLength = footerPayload.remaining();
        // TODO:        IOUtil.writeFully(outputStream, footerPayload);
        outputStream.write(footerPayload.array());
        writeIntegerLittleEndian(outputStream, footerPayloadLength);
        writeFlags();
        outputStream.write(MAGIC);
    }

    private void writeFlags() throws IOException {
        // Currently, iceberg uses 4 bytes for boolean flags. For example,
        // PuffinFormat.Flag.FOOTER_PAYLOAD_COMPRESSED is used to indicate whether the footerPayload
        // is compressed, and this flag uses the first bit in first byte. Other bits in 4 bytes are
        // all 0.
        // As the footerPayload written by paimon is not compressed, the first bit is 0. We can
        // write 0 with int type 4 times directly.
        for (int byteNumber = 0; byteNumber < FOOTER_STRUCT_FLAGS_LENGTH; byteNumber++) {
            int byteFlag = 0;
            // here nly write 1 byte
            outputStream.write(byteFlag);
        }
    }
}
