/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shenyu.web.loader;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.shenyu.common.concurrent.ShenyuThreadFactory;
import org.apache.shenyu.common.config.ShenyuConfig;
import org.apache.shenyu.common.config.ShenyuConfig.ExtPlugin;
import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.plugin.api.ShenyuPlugin;
import org.apache.shenyu.plugin.base.cache.CommonPluginDataSubscriber;
import org.apache.shenyu.plugin.base.handler.PluginDataHandler;
import org.apache.shenyu.web.handler.ShenyuWebHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The type Shenyu loader service.
 */
public class ShenyuLoaderService {

    private static final Logger LOG = LoggerFactory.getLogger(ShenyuLoaderService.class);

    private final ShenyuWebHandler webHandler;

    private final CommonPluginDataSubscriber subscriber;

    private final ShenyuConfig shenyuConfig;

    /**
     * Instantiates a new Shenyu loader service.
     *
     * @param webHandler   the web handler
     * @param subscriber   the subscriber
     * @param shenyuConfig the shenyu config
     */
    public ShenyuLoaderService(final ShenyuWebHandler webHandler, final CommonPluginDataSubscriber subscriber, final ShenyuConfig shenyuConfig) {
        // 插件信息的信息订阅
        this.subscriber = subscriber;
        // Shenyu封装的WebHandler，包含了所有的插件逻辑
        this.webHandler = webHandler;
        // 配置信息
        this.shenyuConfig = shenyuConfig;
        // 扩展插件的配置信息，如路径，是否启用、开启多少线程来处理、检查加载的频率等信息
        ExtPlugin config = shenyuConfig.getExtPlugin();
        // 如果启用的，则创建定时任务来检查并加载
        if (config.getEnabled()) {
            // 创建一个指定线程名称的定时任务
            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(config.getThreads(), ShenyuThreadFactory.create("plugin-ext-loader", true));
            // 创建固定频率执行的任务，默认在30s，每300s，执行一次
            executor.scheduleAtFixedRate(() -> loadExtOrUploadPlugins(null), config.getScheduleDelay(), config.getScheduleTime(), TimeUnit.SECONDS);
        }
    }

    /**
     * loadPlugin from ext-lib or admin upload jar.
     *
     * @param uploadedJarResource uploadedJarResource is null load ext-lib,not null load admin upload jar
     */
    public void loadExtOrUploadPlugins(final PluginData uploadedJarResource) {
        try {
            List<ShenyuLoaderResult> plugins = new ArrayList<>();
            // 获取ShenyuPluginClassloader的持有对象
            ShenyuPluginClassloaderHolder singleton = ShenyuPluginClassloaderHolder.getSingleton();
            if (Objects.isNull(uploadedJarResource)) {
                // 参数为空，则从扩展的目录，加载所有的jar包
                // PluginJar：包含ShenyuPlugin接口、PluginDataHandler接口的数据
                List<PluginJarParser.PluginJar> uploadPluginJars = ShenyuExtPathPluginJarLoader.loadExtendPlugins(shenyuConfig.getExtPlugin().getPath());
                // 遍历所有的待加载插件
                for (PluginJarParser.PluginJar extPath : uploadPluginJars) {
                    LOG.info("shenyu extPlugin find new {} to load", extPath.getAbsolutePath());
                    // 使用扩展插件的加载器来加载指定的插件，便于后续的加载和卸载
                    ShenyuPluginClassLoader extPathClassLoader = singleton.createPluginClassLoader(extPath);
                    // 使用ShenyuPluginClassLoader 进行加载
                    // 主要逻辑是：判断是否实现ShenyuPlugin接口、PluginDataHandler接口 或是否标识 @Component\@Service等注解，如果有，则注册为SpringBean
                    // 构造 ShenyuLoaderResult对象
                    plugins.addAll(extPathClassLoader.loadUploadedJarPlugins());
                }
            } else {
                // 加载指定jar，逻辑同加载全部
                PluginJarParser.PluginJar pluginJar = PluginJarParser.parseJar(Base64.getDecoder().decode(uploadedJarResource.getPluginJar()));
                LOG.info("shenyu upload plugin jar find new {} to load", pluginJar.getJarKey());
                ShenyuPluginClassLoader uploadPluginClassLoader = singleton.createPluginClassLoader(pluginJar);
                plugins.addAll(uploadPluginClassLoader.loadUploadedJarPlugins());
            }
            // 将扩展的插件，加入到ShenyuWebHandler的插件列表，后续的请求则会经过加入的插件内容
            loaderPlugins(plugins);
        } catch (Exception e) {
            LOG.error("shenyu plugins load has error ", e);
        }
    }

    /**
     * loaderPlugins.
     *
     * @param results results
     */
    private void loaderPlugins(final List<ShenyuLoaderResult> results) {
        if (CollectionUtils.isEmpty(results)) {
            return;
        }
        // 获取所有实现了接口ShenyuPlugin的对象
        List<ShenyuPlugin> shenyuExtendPlugins = results.stream().map(ShenyuLoaderResult::getShenyuPlugin).filter(Objects::nonNull).collect(Collectors.toList());
        // 同步更新webHandler中plugins
        webHandler.putExtPlugins(shenyuExtendPlugins);
        // 获取所有实现了接口PluginDataHandler的对象
        List<PluginDataHandler> handlers = results.stream().map(ShenyuLoaderResult::getPluginDataHandler).filter(Objects::nonNull).collect(Collectors.toList());
        // 同步扩展的PluginDataHandler
        subscriber.putExtendPluginDataHandler(handlers);

    }

}
