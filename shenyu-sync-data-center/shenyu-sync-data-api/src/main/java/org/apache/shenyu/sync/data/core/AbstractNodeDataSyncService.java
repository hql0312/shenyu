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

package org.apache.shenyu.sync.data.core;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.shenyu.common.constant.DefaultNodeConstants;
import org.apache.shenyu.common.constant.NacosPathConstants;
import org.apache.shenyu.common.dto.AppAuthData;
import org.apache.shenyu.common.dto.DiscoverySyncData;
import org.apache.shenyu.common.dto.MetaData;
import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.common.dto.ProxySelectorData;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.exception.ShenyuException;
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.sync.data.api.AuthDataSubscriber;
import org.apache.shenyu.sync.data.api.DiscoveryUpstreamDataSubscriber;
import org.apache.shenyu.sync.data.api.MetaDataSubscriber;
import org.apache.shenyu.sync.data.api.PluginDataSubscriber;
import org.apache.shenyu.sync.data.api.ProxySelectorDataSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * AbstractPathDataSyncService.
 * Abstract method to monitor child node changes.
 * 节点类的基类
 */
public abstract class AbstractNodeDataSyncService {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractNodeDataSyncService.class);

    private final ChangeData changeData;

    private final PluginDataSubscriber pluginDataSubscriber;

    private final List<MetaDataSubscriber> metaDataSubscribers;

    private final List<AuthDataSubscriber> authDataSubscribers;

    private final List<ProxySelectorDataSubscriber> proxySelectorDataSubscribers;

    private final List<DiscoveryUpstreamDataSubscriber> discoveryUpstreamDataSubscribers;

    public AbstractNodeDataSyncService(final ChangeData changeData,
                                       final PluginDataSubscriber pluginDataSubscriber,
                                       final List<MetaDataSubscriber> metaDataSubscribers,
                                       final List<AuthDataSubscriber> authDataSubscribers,
                                       final List<ProxySelectorDataSubscriber> proxySelectorDataSubscribers,
                                       final List<DiscoveryUpstreamDataSubscriber> discoveryUpstreamDataSubscribers) {
        // 监听的变更的数据的key
        this.changeData = changeData;
        // pluginData订阅器
        this.pluginDataSubscriber = pluginDataSubscriber;
        // 元数据订阅器
        this.metaDataSubscribers = metaDataSubscribers;
        // 授权数据订阅器
        this.authDataSubscribers = authDataSubscribers;
        // selector订阅器
        this.proxySelectorDataSubscribers = proxySelectorDataSubscribers;
        // 注册发现的下游数据订阅器
        this.discoveryUpstreamDataSubscribers = discoveryUpstreamDataSubscribers;
    }

    // 开始监听
    protected void startWatch() {
        try {
            // 获取所有的插件列表并监听,在Apollo中主要是获取数据，不会回调
            final List<String> pluginNames = getConfigListOnWatch(changeData.getPluginDataId() + DefaultNodeConstants.POINT_LIST, updateData -> {
                List<String> changedPluginNames = GsonUtils.getInstance().fromList(updateData, String.class);
                watcherPlugin(changedPluginNames);
            });

            // 监听列表的变更
            watcherPlugin(pluginNames);

            watchCommonList(changeData.getAuthDataId() + DefaultNodeConstants.JOIN_POINT, this::cacheAuthData, this::unCacheAuthData);

            watchCommonList(changeData.getMetaDataId() + DefaultNodeConstants.JOIN_POINT, this::cacheMetaData, this::unCacheMetaData);

        } catch (Exception e) {
            throw new ShenyuException(e);
        }
    }

    private List<String> getConfigListOnWatch(final String key, final Consumer<String> updateHandler) {
        final String serviceConfig = getServiceConfig(key, updateHandler, null);
        return GsonUtils.getInstance().fromList(serviceConfig, String.class);
    }

    protected abstract String getServiceConfig(String key, Consumer<String> updateHandler, Consumer<String> deleteHandler);

    private String getConfigOnWatch(final String key, final Consumer<String> updateHandler, final Consumer<String> deleteHandler) {
        return getServiceConfig(key, updateHandler, deleteHandler);
    }

    private void removeListener(final String removeKey) {
        LOG.info("AbstractNodeDataSyncService sync remove listener key:{}", removeKey);
        doRemoveListener(removeKey);
    }

    protected abstract void doRemoveListener(String removeKey);

    /**
     * 配置中心的key分为以下几个：
     * plugin.list 插件列表
     * plugin.${pluginname} 单个插件的详细信息
     * selector.${pluginname}.list 单个插件的selector列表
     * selector.${pluginname}.${selectorId} 单个selector 详细信息
     * rule.${pluginname}.${selectorId}.list  单个selector的rule列表
     * rule.${pluginname}.${selectorId}.${ruleId} 单个rule的详细信息
     *
     * @param pluginNames
     */
    private void watcherPlugin(final List<String> pluginNames) {
        if (ObjectUtils.isEmpty(pluginNames)) {
            return;
        }
        // 在配置中心以 plugin.${pluginname} 进行创建key
        // 逐个遍历所有的插件
        pluginNames.forEach(pluginName -> {
            // 获取插件数据
            final String pluginData = this.getConfigOnWatch(changeData.getPluginDataId() + DefaultNodeConstants.JOIN_POINT + pluginName, this::cachePluginData, this::unCachePluginData);
            // 将数据更新到本地缓存
            cachePluginData(pluginData);
            // 获取对应的插件下的所有selector数据
            // selector 对应的key 为 selector.${pluginname}.list，该数据为插件对应的selectorId列表
            // 以下的会处理 selector.${pluginname}.${selectorId} 为selector 详细信息
            final List<String> selectorIds = getConfigListOnWatch(changeData.getSelectorDataId() + DefaultNodeConstants.JOIN_POINT + pluginName + DefaultNodeConstants.POINT_LIST, updateData -> {
                List<String> changedSelectorIds = GsonUtils.getInstance().fromList(updateData, String.class);
                watcherSelector(pluginName, changedSelectorIds);
            });
            // 处理selector数据并更新本地缓存
            // 同时处理rule信息
            watcherSelector(pluginName, selectorIds);

            watchCommonList(NacosPathConstants.PROXY_SELECTOR_DATA_ID + DefaultNodeConstants.JOIN_POINT + pluginName + DefaultNodeConstants.JOIN_POINT,
                    this::cacheProxySelectorData, this::unCacheProxySelectorData);
            watchCommonList(NacosPathConstants.DISCOVERY_DATA_ID + DefaultNodeConstants.JOIN_POINT + pluginName + DefaultNodeConstants.JOIN_POINT,
                    this::cacheProxySelectorData, this::unCacheProxySelectorData);
        });
    }

    /**
     * watchCommonList.
     * examples data:
     * meta.list
     * -> meta.id
     * -> meta.id
     * -> meta.id
     *
     * @param keyPrefix     keyPrefix
     * @param updateHandler updateHandler
     * @param deleteHandler deleteHandler
     */
    private void watchCommonList(final String keyPrefix, final Consumer<String> updateHandler,
                                 final Consumer<String> deleteHandler) {
        final List<String> keyIds = getConfigListOnWatch(keyPrefix + DefaultNodeConstants.LIST_STR, updateData -> {
            List<String> changedIds = GsonUtils.getInstance().fromList(updateData, String.class);
            watcherCommonData(changedIds, keyPrefix, updateHandler, deleteHandler);
        });

        watcherCommonData(keyIds, keyPrefix, updateHandler, deleteHandler);
    }

    private void watcherCommonData(final List<String> keys, final String keyPrefix,
                                   final Consumer<String> updateHandler, final Consumer<String> deleteHandler) {
        if (ObjectUtils.isEmpty(keys)) {
            return;
        }
        keys.forEach(key -> {
            final String keyData = this.getConfigOnWatch(keyPrefix + key, updateHandler, deleteHandler);
            updateHandler.accept(keyData);
        });
    }

    private void watcherSelector(final String pluginName, final List<String> selectorIds) {
        if (ObjectUtils.isEmpty(selectorIds)) {
            return;
        }
        selectorIds.forEach(selectorId -> {
            // selector.${pluginname}.${selectorId}
            final String selectorData = this.getConfigOnWatch(changeData.getSelectorDataId()
                            + DefaultNodeConstants.JOIN_POINT + pluginName + DefaultNodeConstants.JOIN_POINT + selectorId,
                    this::cacheSelectorData, this::unCacheSelectorData);
            // 将数据更新到本地缓存
            cacheSelectorData(selectorData);
            // 获取对应的rule数据列表，key为 rule.${pluginname}.${selectorId}.list
            final List<String> ruleIds = getConfigListOnWatch(changeData.getRuleDataId() + DefaultNodeConstants.JOIN_POINT
                            + pluginName + DefaultNodeConstants.JOIN_POINT + selectorId + DefaultNodeConstants.POINT_LIST,
                    updateData -> {
                        List<String> upSelectorIds = GsonUtils.getInstance().fromList(updateData, String.class);
                        watcherRule(selectorId, upSelectorIds, pluginName);
                    });

            watcherRule(selectorId, ruleIds, pluginName);
        });
    }

    private void watcherRule(final String selectorId, final List<String> ruleIds, final String pluginName) {
        if (ObjectUtils.isEmpty(ruleIds)) {
            return;
        }
        ruleIds.forEach(ruleId -> {
            final String ruleDataStr = this.getConfigOnWatch(changeData.getRuleDataId() + DefaultNodeConstants.JOIN_POINT + pluginName
                            + DefaultNodeConstants.JOIN_POINT + selectorId + DefaultNodeConstants.JOIN_POINT + ruleId,
                    this::cacheRuleData, this::unCacheRuleData);
            // 更新本地缓存
            cacheRuleData(ruleDataStr);
        });
    }

    protected void cachePluginData(final String dataString) {
        final PluginData pluginData = GsonUtils.getInstance().fromJson(dataString, PluginData.class);
        Optional.ofNullable(pluginData)
                .flatMap(data -> Optional.ofNullable(pluginDataSubscriber)).ifPresent(e -> e.onSubscribe(pluginData));
    }

    protected void unCachePluginData(final String pluginName) {
        final PluginData data = new PluginData();
        data.setName(pluginName);
        Optional.ofNullable(pluginDataSubscriber).ifPresent(e -> e.unSubscribe(data));
    }

    protected void cacheSelectorData(final String dataString) {
        final SelectorData selectorData = GsonUtils.getInstance().fromJson(dataString, SelectorData.class);
        Optional.ofNullable(selectorData)
                .ifPresent(data -> Optional.ofNullable(pluginDataSubscriber).ifPresent(e -> e.onSelectorSubscribe(data)));
    }

    protected void unCacheSelectorData(final String removeKey) {
        final SelectorData selectorData = new SelectorData();
        final String[] ruleKeys = StringUtils.split(removeKey, DefaultNodeConstants.JOIN_POINT);
        selectorData.setPluginName(ruleKeys[1]);
        selectorData.setId(ruleKeys[2]);
        Optional.ofNullable(pluginDataSubscriber).ifPresent(e -> e.unSelectorSubscribe(selectorData));
        removeListener(removeKey);
    }

    protected void cacheRuleData(final String dataString) {
        final RuleData ruleData = GsonUtils.getInstance().fromJson(dataString, RuleData.class);
        Optional.ofNullable(ruleData)
                .ifPresent(data -> Optional.ofNullable(pluginDataSubscriber).ifPresent(e -> e.onRuleSubscribe(data)));
    }

    protected void unCacheRuleData(final String removeKey) {
        final RuleData ruleData = new RuleData();
        final String[] ruleKeys = StringUtils.split(removeKey, DefaultNodeConstants.JOIN_POINT);
        ruleData.setPluginName(ruleKeys[1]);
        ruleData.setSelectorId(ruleKeys[2]);
        ruleData.setId(ruleKeys[3]);
        Optional.ofNullable(pluginDataSubscriber).ifPresent(e -> e.unRuleSubscribe(ruleData));
        removeListener(removeKey);
    }

    protected void cacheAuthData(final String dataString) {
        final AppAuthData appAuthData = GsonUtils.getInstance().fromJson(dataString, AppAuthData.class);
        Optional.ofNullable(appAuthData)
                .ifPresent(data -> authDataSubscribers.forEach(e -> e.onSubscribe(data)));
    }

    protected void unCacheAuthData(final String removeKey) {
        final AppAuthData appAuthData = new AppAuthData();
        final String[] ruleKeys = StringUtils.split(removeKey, DefaultNodeConstants.JOIN_POINT);
        appAuthData.setAppKey(ruleKeys[1]);
        authDataSubscribers.forEach(e -> e.unSubscribe(appAuthData));
        removeListener(removeKey);
    }

    protected void cacheMetaData(final String dataString) {
        final MetaData metaData = GsonUtils.getInstance().fromJson(dataString, MetaData.class);
        Optional.ofNullable(metaData)
                .ifPresent(data -> metaDataSubscribers.forEach(e -> e.onSubscribe(metaData)));
    }

    protected void unCacheMetaData(final String removeKey) {
        final MetaData metaData = new MetaData();
        final String[] ruleKeys = StringUtils.split(removeKey, DefaultNodeConstants.JOIN_POINT);
        metaData.setId(ruleKeys[1]);
        metaDataSubscribers.forEach(e -> e.unSubscribe(metaData));
        removeListener(removeKey);
    }

    protected void cacheProxySelectorData(final String dataString) {
        final ProxySelectorData proxySelectorData = GsonUtils.getInstance().fromJson(dataString, ProxySelectorData.class);
        Optional.ofNullable(proxySelectorData)
                .ifPresent(data -> proxySelectorDataSubscribers.forEach(e -> e.onSubscribe(data)));
    }

    protected void unCacheProxySelectorData(final String removeKey) {
        ProxySelectorData proxySelectorData = new ProxySelectorData();
        final String[] proxySelectorKeys = StringUtils.split(removeKey, DefaultNodeConstants.JOIN_POINT);
        proxySelectorData.setPluginName(proxySelectorKeys[2]);
        proxySelectorData.setName(proxySelectorKeys[3]);
        proxySelectorDataSubscribers.forEach(e -> e.unSubscribe(proxySelectorData));
        removeListener(removeKey);
    }

    protected void cacheDiscoveryUpstreamData(final String dataString) {
        final DiscoverySyncData discoverySyncData = GsonUtils.getInstance().fromJson(dataString, DiscoverySyncData.class);
        Optional.ofNullable(discoverySyncData)
                .ifPresent(data -> discoveryUpstreamDataSubscribers.forEach(e -> e.onSubscribe(data)));
    }

    protected void unCacheDiscoveryUpstreamData(final String removeKey) {
        DiscoverySyncData proxySelectorData = new DiscoverySyncData();
        final String[] proxySelectorKeys = StringUtils.split(removeKey, DefaultNodeConstants.JOIN_POINT);
        proxySelectorData.setPluginName(proxySelectorKeys[2]);
        proxySelectorData.setSelectorId(proxySelectorKeys[3]);
        discoveryUpstreamDataSubscribers.forEach(e -> e.unSubscribe(proxySelectorData));
        removeListener(removeKey);
    }

    public static class ChangeData {

        /**
         * plugin data id.
         */
        private final String pluginDataId;

        /**
         * selector data id.
         */
        private final String selectorDataId;

        /**
         * rule data id.
         */
        private final String ruleDataId;

        /**
         * auth data id.
         */
        private final String authDataId;

        /**
         * meta data id.
         */
        private final String metaDataId;

        /**
         * proxySelector data id.
         */
        private final String proxySelectorDataId;

        /**
         * discovery data id.
         */
        private final String discoveryDataId;

        /**
         * ChangeData.
         *
         * @param pluginDataId   pluginDataId
         * @param selectorDataId selectorDataId
         * @param ruleDataId     ruleDataId
         * @param authDataId     authDataId
         * @param metaDataId     metaDataId
         */
        public ChangeData(final String pluginDataId, final String selectorDataId,
                          final String ruleDataId, final String authDataId,
                          final String metaDataId, final String proxySelectorDataId, final String discoveryDataId) {
            this.pluginDataId = pluginDataId;
            this.selectorDataId = selectorDataId;
            this.ruleDataId = ruleDataId;
            this.authDataId = authDataId;
            this.metaDataId = metaDataId;
            this.proxySelectorDataId = proxySelectorDataId;
            this.discoveryDataId = discoveryDataId;
        }

        /**
         * pluginDataId.
         *
         * @return PluginDataId
         */
        public String getPluginDataId() {
            return pluginDataId;
        }

        /**
         * selectorDataId.
         *
         * @return SelectorDataId
         */
        public String getSelectorDataId() {
            return selectorDataId;
        }

        /**
         * ruleDataId.
         *
         * @return RuleDataId
         */
        public String getRuleDataId() {
            return ruleDataId;
        }

        /**
         * authDataId.
         *
         * @return AuthDataId
         */
        public String getAuthDataId() {
            return authDataId;
        }

        /**
         * metaDataId.
         *
         * @return MetaDataId
         */
        public String getMetaDataId() {
            return metaDataId;
        }

        /**
         * get proxySelectorDataId.
         *
         * @return proxySelectorDataId
         */
        public String getProxySelectorDataId() {
            return proxySelectorDataId;
        }

        /**
         * get discoveryDataId.
         *
         * @return discoveryDataId
         */
        public String getDiscoveryDataId() {
            return discoveryDataId;
        }

    }
}
