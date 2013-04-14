/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.unit.threadpool;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.concurrent.EsAbortPolicy;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.testng.annotations.Test;

import java.util.concurrent.*;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 */
public class UpdateThreadPoolSettingsTests {

    private ThreadPool.Info info(ThreadPool threadPool, String name) {
        for (ThreadPool.Info info : threadPool.info()) {
            if (info.name().equals(name)) {
                return info;
            }
        }
        return null;
    }

    @Test
    public void testCachedExecutorType() {
        ThreadPool threadPool = new ThreadPool(ImmutableSettings.settingsBuilder().put("threadpool.search.type", "cached").build(), null);
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("cached"));
        assertThat(info(threadPool, Names.SEARCH).keepAlive().minutes(), equalTo(5L));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));

        // Replace with different type
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.type", "same").build());
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("same"));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(ListeningExecutorService.class));

        // Replace with different type again
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.type", "scaling")
                .put("threadpool.search.keep_alive", "10m")
                .build());
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("scaling"));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(1));
        // Make sure keep alive value changed
        assertThat(info(threadPool, Names.SEARCH).keepAlive().minutes(), equalTo(10L));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(10L));

        // Put old type back
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.type", "cached").build());
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("cached"));
        // Make sure keep alive value reused
        assertThat(info(threadPool, Names.SEARCH).keepAlive().minutes(), equalTo(10L));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));

        // Change keep alive
        Executor oldExecutor = threadPool.executor(Names.SEARCH);
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.keep_alive", "1m").build());
        // Make sure keep alive value changed
        assertThat(info(threadPool, Names.SEARCH).keepAlive().minutes(), equalTo(1L));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(1L));
        // Make sure executor didn't change
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("cached"));
        assertThat(threadPool.executor(Names.SEARCH), sameInstance(oldExecutor));

        // Set the same keep alive
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.keep_alive", "1m").build());
        // Make sure keep alive value didn't change
        assertThat(info(threadPool, Names.SEARCH).keepAlive().minutes(), equalTo(1L));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(1L));
        // Make sure executor didn't change
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("cached"));
        assertThat(threadPool.executor(Names.SEARCH), sameInstance(oldExecutor));

        threadPool.shutdown();
    }

    @Test
    public void testFixedExecutorType() {
        ThreadPool threadPool = new ThreadPool(settingsBuilder().put("threadpool.search.type", "fixed").build(), null);
        assertThat(info(threadPool, Names.SEARCH).rejectSetting(), equalTo("abort"));
        assertThat(info(threadPool, Names.SEARCH).queueType(), equalTo("linked"));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));

        // Replace with different type
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.type", "scaling")
                .put("threadpool.search.keep_alive", "10m")
                .put("threadpool.search.min", "2")
                .put("threadpool.search.size", "15")
                .build());
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("scaling"));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(2));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getMaximumPoolSize(), equalTo(15));
        assertThat(info(threadPool, Names.SEARCH).min(), equalTo(2));
        assertThat(info(threadPool, Names.SEARCH).max(), equalTo(15));
        // Make sure keep alive value changed
        assertThat(info(threadPool, Names.SEARCH).keepAlive().minutes(), equalTo(10L));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(10L));

        // Put old type back
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.type", "fixed")
                .build());
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("fixed"));
        // Make sure keep alive value is not used
        assertThat(info(threadPool, Names.SEARCH).keepAlive(), nullValue());
        // Make sure keep pool size value were reused
        assertThat(info(threadPool, Names.SEARCH).min(), equalTo(15));
        assertThat(info(threadPool, Names.SEARCH).max(), equalTo(15));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(15));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getMaximumPoolSize(), equalTo(15));

        // Change size
        Executor oldExecutor = threadPool.executor(Names.SEARCH);
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.size", "10").build());
        // Make sure size values changed
        assertThat(info(threadPool, Names.SEARCH).max(), equalTo(10));
        assertThat(info(threadPool, Names.SEARCH).min(), equalTo(10));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getMaximumPoolSize(), equalTo(10));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(10));
        // Make sure executor didn't change
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("fixed"));
        assertThat(threadPool.executor(Names.SEARCH), sameInstance(oldExecutor));

        // Change queue capacity
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.queue", "500")
                .build());
        assertThat(info(threadPool, Names.SEARCH).queueType(), equalTo("linked"));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getQueue(), instanceOf(LinkedBlockingQueue.class));

        // Set different queue and size type
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.queue_type", "array")
                .put("threadpool.search.size", "12")
                .build());
        // Make sure keep size changed
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("fixed"));
        assertThat(info(threadPool, Names.SEARCH).max(), equalTo(12));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(12));
        assertThat(info(threadPool, Names.SEARCH).queueType(), equalTo("array"));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getQueue(), instanceOf(ArrayBlockingQueue.class));

        // Change rejection policy
        oldExecutor = threadPool.executor(Names.SEARCH);
        assertThat(info(threadPool, Names.SEARCH).rejectSetting(), equalTo("abort"));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getRejectedExecutionHandler(), instanceOf(EsAbortPolicy.class));
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.reject_policy", "caller").build());
        // Make sure rejection handler changed
        assertThat(info(threadPool, Names.SEARCH).rejectSetting(), equalTo("caller"));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getRejectedExecutionHandler(), instanceOf(ThreadPoolExecutor.CallerRunsPolicy.class));
        // Make sure executor didn't change
        assertThat(threadPool.executor(Names.SEARCH), sameInstance(oldExecutor));

        threadPool.shutdown();
    }


    @Test
    public void testScalingExecutorType() {
        ThreadPool threadPool = new ThreadPool(
                settingsBuilder().put("threadpool.search.type", "scaling").put("threadpool.search.size", 10).build(), null);
        assertThat(info(threadPool, Names.SEARCH).min(), equalTo(1));
        assertThat(info(threadPool, Names.SEARCH).max(), equalTo(10));
        assertThat(info(threadPool, Names.SEARCH).keepAlive().minutes(), equalTo(5L));
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("scaling"));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));

        // Change settings that doesn't require pool replacement
        Executor oldExecutor = threadPool.executor(Names.SEARCH);
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.type", "scaling")
                .put("threadpool.search.keep_alive", "10m")
                .put("threadpool.search.min", "2")
                .put("threadpool.search.size", "15")
                .build());
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("scaling"));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(2));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getMaximumPoolSize(), equalTo(15));
        assertThat(info(threadPool, Names.SEARCH).min(), equalTo(2));
        assertThat(info(threadPool, Names.SEARCH).max(), equalTo(15));
        // Make sure keep alive value changed
        assertThat(info(threadPool, Names.SEARCH).keepAlive().minutes(), equalTo(10L));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(10L));
        assertThat(threadPool.executor(Names.SEARCH), sameInstance(oldExecutor));

        threadPool.shutdown();
    }

    @Test
    public void testBlockingExecutorType() {
        ThreadPool threadPool = new ThreadPool(settingsBuilder().put("threadpool.search.type", "blocking").put("threadpool.search.size", "10").build(), null);
        assertThat(info(threadPool, Names.SEARCH).min(), equalTo(1));
        assertThat(info(threadPool, Names.SEARCH).max(), equalTo(10));
        assertThat(info(threadPool, Names.SEARCH).capacity().singles(), equalTo(1000L));
        assertThat(info(threadPool, Names.SEARCH).waitTime().minutes(), equalTo(1L));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));

        // Replace with different type
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.type", "scaling")
                .put("threadpool.search.keep_alive", "10m")
                .put("threadpool.search.min", "2")
                .put("threadpool.search.size", "15")
                .build());
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("scaling"));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(2));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getMaximumPoolSize(), equalTo(15));
        assertThat(info(threadPool, Names.SEARCH).min(), equalTo(2));
        assertThat(info(threadPool, Names.SEARCH).max(), equalTo(15));
        // Make sure keep alive value changed
        assertThat(info(threadPool, Names.SEARCH).keepAlive().minutes(), equalTo(10L));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(10L));

        // Put old type back
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.type", "blocking")
                .build());
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("blocking"));
        // Make sure keep alive value is not used
        assertThat(info(threadPool, Names.SEARCH).keepAlive().minutes(), equalTo(10L));
        // Make sure keep pool size value were reused
        assertThat(info(threadPool, Names.SEARCH).min(), equalTo(2));
        assertThat(info(threadPool, Names.SEARCH).max(), equalTo(15));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(2));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getMaximumPoolSize(), equalTo(15));

        // Change size
        Executor oldExecutor = threadPool.executor(Names.SEARCH);
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.size", "10").put("threadpool.search.min", "5").build());
        // Make sure size values changed
        assertThat(info(threadPool, Names.SEARCH).min(), equalTo(5));
        assertThat(info(threadPool, Names.SEARCH).max(), equalTo(10));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(5));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getMaximumPoolSize(), equalTo(10));
        // Make sure executor didn't change
        assertThat(info(threadPool, Names.SEARCH).type(), equalTo("blocking"));
        assertThat(threadPool.executor(Names.SEARCH), sameInstance(oldExecutor));

        // Change queue capacity
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.queue_size", "500")
                .build());
        assertThat(info(threadPool, Names.SEARCH).capacity().singles(), equalTo(500L));

        // Change wait time capacity
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.wait_time", "2m")
                .build());
        assertThat(info(threadPool, Names.SEARCH).waitTime().minutes(), equalTo(2L));

        threadPool.shutdown();
    }

    @Test(timeOut = 10000)
    public void testShutdownDownNowDoesntBlock() throws Exception {
        ThreadPool threadPool = new ThreadPool(ImmutableSettings.settingsBuilder().put("threadpool.search.type", "cached").build(), null);

        final CountDownLatch latch = new CountDownLatch(1);
        Executor oldExecutor = threadPool.executor(Names.SEARCH);
        threadPool.executor(Names.SEARCH).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(20000);
                } catch (InterruptedException ex) {
                    latch.countDown();
                    Thread.currentThread().interrupt();
                }
            }
        });
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.type", "fixed").build());
        assertThat(threadPool.executor(Names.SEARCH), not(sameInstance(oldExecutor)));
        assertThat(((ThreadPoolExecutor) oldExecutor).isShutdown(), equalTo(true));
        assertThat(((ThreadPoolExecutor) oldExecutor).isTerminating(), equalTo(true));
        assertThat(((ThreadPoolExecutor) oldExecutor).isTerminated(), equalTo(false));
        threadPool.shutdownNow();
        latch.await();
    }

}
