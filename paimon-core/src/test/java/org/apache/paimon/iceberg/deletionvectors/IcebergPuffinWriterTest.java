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

import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/** doc. */
public class IcebergPuffinWriterTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void test() throws IOException {
        HashSet<Integer> toDelete = new HashSet<>();
        //        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            if (i % 5 == 0) {
                toDelete.add(i);
            }
        }
        HashSet<Integer> notDelete = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            if (!toDelete.contains(i)) {
                notDelete.add(i);
            }
        }

        BitmapDeletionVector deletionVector = new BitmapDeletionVector();
        assertThat(deletionVector.isEmpty()).isTrue();

        for (Integer i : toDelete) {
            assertThat(deletionVector.checkedDelete(i)).isTrue();
            assertThat(deletionVector.checkedDelete(i)).isFalse();
        }

        IcebergPositionDeleteIndex icebergPositionDeleteIndex =
                new IcebergPositionDeleteIndex(deletionVector.get());
        IcebergBlob icebergBlob =
                new IcebergBlob(
                        "deletion-vector-v1",
                        Collections.singletonList(Integer.MAX_VALUE - 2),
                        -1,
                        -1,
                        icebergPositionDeleteIndex.serialize(),
                        Collections.emptyMap());

        try (IcebergPuffinWriter puffinWriter = createPuffinWriter()) {
            puffinWriter.write(icebergBlob);
        }

        InMemoryInputFile inputFile =
                new InMemoryInputFile(
                        Files.readAllBytes(
                                Paths.get(
                                        "/Users/catyeah/testHome/iceberg/puffin/"
                                                + "testIceberg.puffin")));
        Puffin.ReadBuilder readBuilder = Puffin.read(inputFile).withFileSize(inputFile.getLength());
        try (PuffinReader reader = readBuilder.build()) {
            FileMetadata fileMetadata = reader.fileMetadata();
        }

        System.out.println("test iceberg puffin.");
    }

    private IcebergPuffinWriter createPuffinWriter() throws IOException {
        FileIO fileIO = LocalFileIO.create();
        return new IcebergPuffinWriter(
                fileIO, new Path("/Users/catyeah/testHome/iceberg/puffin/" + "testIceberg.puffin"));
    }
}
