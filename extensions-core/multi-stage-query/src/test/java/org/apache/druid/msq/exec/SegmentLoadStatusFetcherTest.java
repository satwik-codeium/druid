/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.sql.client.BrokerClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlTaskStatus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SegmentLoadStatusFetcherTest
{
  private static final String TEST_DATASOURCE = "testDatasource";

  private SegmentLoadStatusFetcher segmentLoadWaiter;
  private BrokerClient brokerClient;

  /**
   * Single version created, loaded after 3 attempts
   */
  @Test
  public void testSingleVersionWaitsForLoadCorrectly() throws Exception
  {
    brokerClient = mock(BrokerClient.class);
    final int[] timesInvoked = {0};

    when(brokerClient.submitSqlTask(any(SqlQuery.class))).thenAnswer(invocation -> {
      timesInvoked[0]++;
      Map<String, Object> resultMap = new HashMap<>();
      resultMap.put("usedSegments", 5);
      resultMap.put("precachedSegments", timesInvoked[0]);
      resultMap.put("onDemandSegments", 0);
      resultMap.put("pendingSegments", 5 - timesInvoked[0]);
      resultMap.put("unknownSegments", 0);

      SqlTaskStatus status = new SqlTaskStatus(ImmutableList.of(resultMap), null);
      return Futures.immediateFuture(status);
    });

    segmentLoadWaiter = new SegmentLoadStatusFetcher(
        brokerClient,
        new ObjectMapper(),
        TEST_DATASOURCE,
        IntStream.range(0, 5)
                .boxed()
                .map(partitionNum -> createTestDataSegment("version1", partitionNum))
                .collect(Collectors.toSet())
    );

    segmentLoadWaiter.waitForSegmentsToLoad();
    verify(brokerClient, times(5)).submitSqlTask(any());
  }

  @Test
  public void testMultipleVersionWaitsForLoadCorrectly() throws Exception
  {
    brokerClient = mock(BrokerClient.class);
    final int[] timesInvoked = {0};

    when(brokerClient.submitSqlTask(any(SqlQuery.class))).thenAnswer(invocation -> {
      timesInvoked[0]++;
      Map<String, Object> resultMap = new HashMap<>();
      resultMap.put("usedSegments", 10);
      resultMap.put("precachedSegments", timesInvoked[0]);
      resultMap.put("onDemandSegments", 0);
      resultMap.put("pendingSegments", 10 - timesInvoked[0]);
      resultMap.put("unknownSegments", 0);

      SqlTaskStatus status = new SqlTaskStatus(ImmutableList.of(resultMap), null);
      return Futures.immediateFuture(status);
    });

    segmentLoadWaiter = new SegmentLoadStatusFetcher(
        brokerClient,
        new ObjectMapper(),
        TEST_DATASOURCE,
        IntStream.range(0, 10)
                .boxed()
                .map(partitionNum -> createTestDataSegment(
                    partitionNum < 5 ? "version1" : "version2",
                    partitionNum
                ))
                .collect(Collectors.toSet())
    );

    segmentLoadWaiter.waitForSegmentsToLoad();
    verify(brokerClient, times(10)).submitSqlTask(any());
  }

  private static DataSegment createTestDataSegment(String version, int partitionNum)
  {
    return DataSegment.builder()
                     .dataSource(TEST_DATASOURCE)
                     .interval(Intervals.of("2020/2021"))
                     .version(version)
                     .shardSpec(new NumberedShardSpec(partitionNum, 0))
                     .build();
  }
}
