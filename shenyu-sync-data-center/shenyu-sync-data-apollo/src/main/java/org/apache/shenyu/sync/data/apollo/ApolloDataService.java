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
        super(new ChangeData(ApolloPathConstants.PLUGIN_DATA_ID,
                        ApolloPathConstants.SELECTOR_DATA_ID,
                        ApolloPathConstants.RULE_DATA_ID,
                        ApolloPathConstants.AUTH_DATA_ID,
                        ApolloPathConstants.META_DATA_ID,
                        ApolloPathConstants.PROXY_SELECTOR_DATA_ID,
                        ApolloPathConstants.DISCOVERY_DATA_ID),
                pluginDataSubscriber, metaDataSubscribers, authDataSubscribers, proxySelectorDataSubscribers, discoveryUpstreamDataSubscribers);
        this.configService = configService;
        // 主要逻辑是更新apollo配置中心的数据到本地缓存
        startWatch();
        // 监听逻辑
        apolloWatchPrefixes();
    }

    /**
     * 1. 不监听结尾是list的key的数据
     * 2. 分别处理plugin\selector\rule\auth\meta\proxy.selector\discovery的变更数据
     */
    private void apolloWatchPrefixes() {
        final ConfigChangeListener listener = changeEvent -> {
            changeEvent.changedKeys().forEach(changeKey -> {
                try {
                    final ConfigChange configChange = changeEvent.getChange(changeKey);
                    if (configChange == null) {
                        LOG.error("apollo watchPrefixes error configChange is null {}", changeKey);
                        return;
                    }
                    final String newValue = configChange.getNewValue();
                    // skip last is "list"
                    final int lastListStrIndex = changeKey.length() - DefaultNodeConstants.LIST_STR.length();
                    if (changeKey.lastIndexOf(DefaultNodeConstants.LIST_STR) == lastListStrIndex) {
                        return;
                    }
                    // check prefix
                    if (changeKey.indexOf(ApolloPathConstants.PLUGIN_DATA_ID) == 0) {
                        if (PropertyChangeType.DELETED.equals(configChange.getChangeType())) {
                            unCachePluginData(changeKey);
                        } else {
                            cachePluginData(newValue);
                        }
                    } else if (changeKey.indexOf(ApolloPathConstants.SELECTOR_DATA_ID) == 0) {
                        if (PropertyChangeType.DELETED.equals(configChange.getChangeType())) {
                            unCacheSelectorData(changeKey);
                        } else {
                            cacheSelectorData(newValue);
                        }
                    } else if (changeKey.indexOf(ApolloPathConstants.RULE_DATA_ID) == 0) {
                        if (PropertyChangeType.DELETED.equals(configChange.getChangeType())) {
                            unCacheRuleData(changeKey);
                        } else {
                            cacheRuleData(newValue);
                        }
                    } else if (changeKey.indexOf(ApolloPathConstants.AUTH_DATA_ID) == 0) {
                        if (PropertyChangeType.DELETED.equals(configChange.getChangeType())) {
                            unCacheAuthData(changeKey);
                        } else {
                            cacheAuthData(newValue);
                        }
                    } else if (changeKey.indexOf(ApolloPathConstants.META_DATA_ID) == 0) {
                        if (PropertyChangeType.DELETED.equals(configChange.getChangeType())) {
                            unCacheMetaData(changeKey);
                        } else {
                            cacheMetaData(newValue);
                        }
                    } else if (changeKey.indexOf(ApolloPathConstants.PROXY_SELECTOR_DATA_ID) == 0) {
                        if (PropertyChangeType.DELETED.equals(configChange.getChangeType())) {
                            unCacheProxySelectorData(changeKey);
                        } else {
                            cacheProxySelectorData(newValue);
                        }
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
