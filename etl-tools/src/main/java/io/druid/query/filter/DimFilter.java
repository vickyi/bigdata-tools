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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.java.util.common.Cacheable;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
})
public interface DimFilter extends Cacheable {
    /**
     * @return Returns an optimized filter.
     * returning the same filter can be a straightforward default implementation.
     */
    DimFilter optimize();

    /**
     * Returns a Filter that implements this DimFilter. This does not generally involve optimizing the DimFilter,
     * so it does make sense to optimize first and then call toFilter on the resulting DimFilter.
     *
     * @return a Filter that implements this DimFilter, or null if this DimFilter is a no-op.
     */
    Filter toFilter();
}
