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

package org.apache.cassandra.transport;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import static org.apache.cassandra.transport.Server.awaitGracefulDisconnect;
import static org.assertj.core.api.Assertions.assertThat;

public class ServerGracefulDisconnectTest
{
    @Test
    public void completesImmediatelyWithNoChannels()
    {
        DefaultChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        assertThat(awaitGracefulDisconnect(channels, 1000)).isEqualTo(0);
    }

    @Test
    public void completesWhenChannelClosesWithinGracePeriod() throws Exception
    {
        DefaultChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        EmbeddedChannel channel = new EmbeddedChannel();
        channels.add(channel);

        Future<Integer> result = CompletableFuture.supplyAsync(() -> awaitGracefulDisconnect(channels, 5000));

        assertThat(result.isDone()).isFalse();

        channel.close();

        assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo(0);
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    public void forceClosesChannelAfterGracePeriod()
    {
        DefaultChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        EmbeddedChannel channel = new EmbeddedChannel();
        channels.add(channel);

        assertThat(awaitGracefulDisconnect(channels, 100)).isEqualTo(1);
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    public void waitsForAllChannelsToClose() throws Exception
    {
        DefaultChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        EmbeddedChannel channel1 = new EmbeddedChannel();
        EmbeddedChannel channel2 = new EmbeddedChannel();
        channels.add(channel1);
        channels.add(channel2);

        Future<Integer> result = CompletableFuture.supplyAsync(() -> awaitGracefulDisconnect(channels, 5000));

        channel1.close();
        assertThat(result.isDone()).isFalse();

        channel2.close();

        assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo(0);
        assertThat(channel1.isOpen()).isFalse();
        assertThat(channel2.isOpen()).isFalse();
    }

    @Test
    public void onlyForceClosesUncooperativeChannels() throws Exception
    {
        DefaultChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        EmbeddedChannel cooperative = new EmbeddedChannel(DefaultChannelId.newInstance());
        EmbeddedChannel uncooperative = new EmbeddedChannel(DefaultChannelId.newInstance());
        channels.add(cooperative);
        channels.add(uncooperative);

        Future<Integer> result = CompletableFuture.supplyAsync(() -> awaitGracefulDisconnect(channels, 200));

        cooperative.close();

        assertThat(result.get(5, TimeUnit.SECONDS))
                  .as("Only the uncooperative channel should be force closed")
                  .isEqualTo(1);
        assertThat(cooperative.isOpen()).isFalse();
        assertThat(uncooperative.isOpen()).isFalse();
    }

    @Test
    public void channelAlreadyClosedBeforeWaitStarts()
    {
        DefaultChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        EmbeddedChannel alreadyClosed = new EmbeddedChannel();
        alreadyClosed.close();
        channels.add(alreadyClosed);

        assertThat(awaitGracefulDisconnect(channels, 1000)).isEqualTo(0);
        assertThat(alreadyClosed.isOpen()).isFalse();
    }
}
