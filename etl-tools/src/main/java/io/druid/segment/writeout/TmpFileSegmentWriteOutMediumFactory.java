/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *http://www.apache.org/licenses/LICENSE-2.0
 *Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.writeout;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.File;
import java.io.IOException;

public final class TmpFileSegmentWriteOutMediumFactory implements SegmentWriteOutMediumFactory {
    private static final TmpFileSegmentWriteOutMediumFactory INSTANCE = new TmpFileSegmentWriteOutMediumFactory();

    private TmpFileSegmentWriteOutMediumFactory() {
    }

    @JsonCreator
    public static TmpFileSegmentWriteOutMediumFactory instance() {
        return INSTANCE;
    }

    @Override
    public SegmentWriteOutMedium makeSegmentWriteOutMedium(File outDir) throws IOException {
        return new TmpFileSegmentWriteOutMedium(outDir);
    }
}
