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
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.iceberg.manifest.IcebergDeleteFileMeta;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** doc. */
public class IcebergDVFileWriter {

    private static final String REFERENCED_DATA_FILE_KEY = "referenced-data-file";
    private static final String CARDINALITY_KEY = "cardinality";

    // input, write the paimon dvs to a puffin file which can be read by iceberg
    Map<String, DeletionVector> deletionVectors = new HashMap<>();
    // output
    Map<String, IcebergDeleteFileMeta> deleteFiles = new HashMap<>();

    public void write(Map<String, DeletionVector> deletionVectors) {}

    private void newPuffinWriter() {}

    private IcebergBlob toIcebergBlob(String path, DeletionVector deletionVector) {
        BitmapDeletionVector bitmapDeletionVector = (BitmapDeletionVector) deletionVector;
        IcebergPositionDeleteIndex icebergPositionDeleteIndex =
                new IcebergPositionDeleteIndex(bitmapDeletionVector.get());

        return new IcebergBlob(
                "deletion-vector-v1",
                Collections.singletonList(Integer.MAX_VALUE - 2),
                -1,
                -1,
                icebergPositionDeleteIndex.serialize(),
                ImmutableMap.of(
                        REFERENCED_DATA_FILE_KEY,
                        path,
                        CARDINALITY_KEY,
                        String.valueOf(icebergPositionDeleteIndex.getCardinality())));
    }
}
