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
package com.datastax.driver.core;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadFactory;

import static com.datastax.driver.core.CCMBridge.ipOfNode;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class NettyOptionsTest {

    @DataProvider(name = "NettyOptionsTest")
    public static Object[][] parameters() {
        Object[][] params = new Object[][]{{1, 1}, {3, 4}};
        return params;
    }

    @Test(groups = "short", dataProvider = "NettyOptionsTest")
    public void should_invoke_netty_options_hooks(int hosts, int coreConnections) throws Exception {
        //given
        int expectedNumberOfCalls = coreConnections * hosts + 1;
        CCMBridge ccm = CCMBridge.builder("test").withNodes(hosts).withCassandraVersion("2.0.10").build();
        Cluster cluster = null;
        try {
            NettyOptions nettyOptions = mock(NettyOptions.class, CALLS_REAL_METHODS.get());
            EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
            Timer timer = new HashedWheelTimer();
            doReturn(eventLoopGroup).when(nettyOptions).eventLoopGroup(any(ThreadFactory.class));
            doReturn(timer).when(nettyOptions).timer(any(ThreadFactory.class));
            final ChannelHandler handler = mock(ChannelHandler.class);
            doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                    SocketChannel channel = (SocketChannel) invocation.getArguments()[0];
                    channel.pipeline().addLast("test-handler", handler);
                    return null;
                }
            }).when(nettyOptions).afterChannelInitialized(any(SocketChannel.class));
            cluster = Cluster.builder()
                    .addContactPoint(ipOfNode(1))
                    .withPoolingOptions(new PoolingOptions()
                                    .setCoreConnectionsPerHost(HostDistance.LOCAL, coreConnections)
                    )
                    .withNettyOptions(nettyOptions)
                    .build();
            // when
            cluster.connect();// force session creation to populate pools
            cluster.close();
            // then
            verify(nettyOptions, times(1)).eventLoopGroup(any(ThreadFactory.class));
            verify(nettyOptions, times(1)).channelClass();
            verify(nettyOptions, times(1)).timer(any(ThreadFactory.class));
            // per-connection hooks will be called coreConnections * hosts + 1 times:
            // the extra call is for the control connection
            verify(nettyOptions, times(expectedNumberOfCalls)).afterBootstrapInitialized(any(Bootstrap.class));
            verify(nettyOptions, times(expectedNumberOfCalls)).afterChannelInitialized(any(SocketChannel.class));
            verify(handler, times(expectedNumberOfCalls)).handlerAdded(any(ChannelHandlerContext.class));
            verify(handler, times(expectedNumberOfCalls)).handlerRemoved(any(ChannelHandlerContext.class));
            verify(nettyOptions, times(1)).onClusterClose(eventLoopGroup);
            verify(nettyOptions, times(1)).onClusterClose(timer);
            verifyNoMoreInteractions(nettyOptions);
        } finally {
            if (cluster != null)
                cluster.close();
            ccm.remove();
        }
    }

}