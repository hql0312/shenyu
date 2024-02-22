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

package org.apache.shenyu.sync.data.apollo;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.enums.PropertyChangeType;
import com.ctrip.framework.apollo.model.ConfigChange;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.shenyu.common.constant.ApolloPathConstants;
import org.apache.shenyu.common.constant.DefaultNodeConstants;
import org.apache.shenyu.sync.data.api.AuthDataSubscriber;
import org.apache.shenyu.sync.data.api.DiscoveryUpstreamDataSubscriber;
import org.apache.shenyu.sync.data.api.MetaDataSubscriber;
import org.apache.shenyu.sync.data.api.PluginDataSubscriber;
import org.apache.shenyu.sync.data.api.ProxySelectorDataSubscriber;
import org.apache.shenyu.sync.data.api.SyncDataService;
import org.apache.shenyu.sync.data.core.AbstractNodeDataSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class ApolloDataService extends AbstractNodeDataSyncService implements SyncDataService {

    private static final Logger LOG = LoggerFactory.getLogger(ApolloDataService.class);

    private final Config configService;

    private ConfigChangeListener watchConfigChangeListener;

    /**
     * Instantiates a new Nacos sync data service.
     *
     * @param configService        the config service
     * @param pluginDataSubscriber the plugin data subscriber
     * @param metaDataSubscribers  the meta data subscribers
     * @param authDataSubscribers  the auth data subscribers
     */
    public ApolloDataService(final Config configService, final PluginDataSubscriber pluginDataSubscriber,
                             final List<MetaDataSubscriber> metaDataSubscribers,
                             final List<AuthDataSubscriber> authDataSubscribers,
                             final List<ProxySelectorDataSubscriber> proxySelectorDataSubscribers,
                             final List<DiscoveryUpstreamDataSubscriber> discoveryUpstreamDataSubscribers) {
        // 配置监听的前缀
        super(new ChangeData(ApolloPathConstants.PLUGIN_DATA_ID,
                        ApolloPathConstants.SELECTOR_DATA_ID,
                        ApolloPathConstants.RULE_DATA_ID,
                        ApolloPathConstants.AUTH_DATA_ID,
                        ApolloPathConstants.META_DATA_ID,
                        ApolloPathConstants.PROXY_SELECTOR_DATA_ID,
                        ApolloPathConstants.DISCOVERY_DATA_ID),
                pluginDataSubscriber, metaDataSubscribers, authDataSubscribers, proxySelectorDataSubscribers, discoveryUpstreamDataSubscribers);
        this.configService = configService;
        // 开始监听
        // 注：Apollo该方法，只负责获取apollo的数据获取，并添加到本地缓存中，不处理监听
        startWatch();
        // 配置监听
        apolloWatchPrefixes();
    }

    /**
     * 1. 不监听结尾是list的key的数据
     * 2. 分别处理plugin\selector\rule\auth\meta\proxy.selector\discovery的变更数据
     */
    private void apolloWatchPrefixes() {
        // 定义监听器
        final ConfigChangeListener listener = changeEvent -> {
            changeEvent.changedKeys().forEach(changeKey -> {
                try {
                    final ConfigChange configChange = changeEvent.getChange(changeKey);
                    // 未变更则跳过
                    if (configChange == null) {
                        LOG.error("apollo watchPrefixes error configChange is null {}", changeKey);
                        return;
                    }
                    final String newValue = configChange.getNewValue();
                    // 如果是list结尾的Key，如plugin.list则跳过，因为这里只是记录生效的一个列表，不会在本地缓存中
                    final int lastListStrIndex = changeKey.length() - DefaultNodeConstants.LIST_STR.length();
                    if (changeKey.lastIndexOf(DefaultNodeConstants.LIST_STR) == lastListStrIndex) {
                        return;
                    }
                    // 如果是plugin.开头 => 处理插件数据
                    if (changeKey.indexOf(ApolloPathConstants.PLUGIN_DATA_ID) == 0) {
                        // 删除
                        if (PropertyChangeType.DELETED.equals(configChange.getChangeType())) {
                            // 清除缓存
                            unCachePluginData(changeKey);
                        } else {
                            // 更新缓存
                            cachePluginData(newValue);
                        }
                        // 如果是selector.开头 => 处理选择器数据
                    } else if (changeKey.indexOf(ApolloPathConstants.SELECTOR_DATA_ID) == 0) {
                        if (PropertyChangeType.DELETED.equals(configChange.getChangeType())) {
                            unCacheSelectorData(changeKey);
                        } else {
                            cacheSelectorData(newValue);
                        }
                        // 如果是rule.开头 => 处理规则数据
                    } else if (changeKey.indexOf(ApolloPathConstants.RULE_DATA_ID) == 0) {
                        if (PropertyChangeType.DELETED.equals(configChange.getChangeType())) {
                            unCacheRuleData(changeKey);
                        } else {
                            cacheRuleData(newValue);
                        }
                        // 如果是auth.开头 => 处理授权数据
                    } else if (changeKey.indexOf(ApolloPathConstants.AUTH_DATA_ID) == 0) {
                        if (PropertyChangeType.DELETED.equals(configChange.getChangeType())) {
                            unCacheAuthData(changeKey);
                        } else {
                            cacheAuthData(newValue);
                        }
                        // 如果是meta.开头 => 处理元数据
                    } else if (changeKey.indexOf(ApolloPathConstants.META_DATA_ID) == 0) {
                        if (PropertyChangeType.DELETED.equals(configChange.getChangeType())) {
                            unCacheMetaData(changeKey);
                        } else {
                            cacheMetaData(newValue);
                        }
                        // 如果是proxy.selector.开头 => 处理代理选择器数据
                    } else if (changeKey.indexOf(ApolloPathConstants.PROXY_SELECTOR_DATA_ID) == 0) {
                        if (PropertyChangeType.DELETED.equals(configChange.getChangeType())) {
                            unCacheProxySelectorData(changeKey);
                        } else {
                            cacheProxySelectorData(newValue);
                        }
                        // 如果是discovery.开头 => 处理下游列表数据
                    } else if (changeKey.indexOf(ApolloPathConstants.DISCOVERY_DATA_ID) == 0) {
                        if (PropertyChangeType.DELETED.equals(configChange.getChangeType())) {
                            unCacheDiscoveryUpstreamData(changeKey);
                        } else {
                            cacheDiscoveryUpstreamData(newValue);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("apollo sync listener change key handler error", e);
                }
            });
        };
        watchConfigChangeListener = listener;
        // 添加监听
        configService.addChangeListener(listener, Collections.emptySet(), ApolloPathConstants.pathKeySet());

    }

    @Override
    protected void doRemoveListener(final String key) {
        // No need to implement
    }

    @Override
    protected String getServiceConfig(final String key, final Consumer<String> updateHandler, final Consumer<String> deleteHandler) {
        return configService.getProperty(key, null);
    }

    /**
     * close.
     */
    @Override
    public void close() {
        if (!ObjectUtils.isEmpty(watchConfigChangeListener)) {
            configService.removeChangeListener(watchConfigChangeListener);
        }
    }
}
