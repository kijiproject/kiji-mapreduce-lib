/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.mapreduce.lib.graph;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.kiji.mapreduce.lib.avro.Edge;

public class TestEdgeWeightEdgeComparator {
  @Test
  public void testSort() {
    EdgeWeightEdgeComparator comparator = new EdgeWeightEdgeComparator();
    List<Edge> edges = TestEdgeUtils.getEdges();
    Collections.sort(edges, comparator);

    assertEquals("carrot", edges.get(0).getLabel().toString());
    assertEquals("banana", edges.get(1).getLabel().toString());
    assertEquals("apple", edges.get(2).getLabel().toString());
  }
}
