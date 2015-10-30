package com.gospelye.routing;

import org.apache.commons.lang.math.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import javax.sql.DataSource;
import java.util.*;

/**
 *
 * 数据源容器,将多个数据源保存其中,为数据源路由器提供数据源
 * Created by yewenhai on 15/10/30.
 */
public class DynamicDataSourceHolder extends AbstractRoutingDataSource{


    private static final ThreadLocal<String> dataSourceHolder = new ThreadLocal<>();


    /** 从库数据源key **/
    private List<String> slaveDataSourcesKeys;

    /** 主库数据源key **/
    private List<String> masterDataSourcesKeys;

    /** 从库数据源 **/
    private Map<String, DataSource> slaveDataSources;

    /** 主库数据源 **/
    private Map<String, DataSource> masterDataSouces;

    private static final Logger log = LoggerFactory.getLogger(DynamicDataSourceHolder.class);


    @Override
    protected Object determineCurrentLookupKey() {
        return dataSourceHolder.get();
    }


    @Override
    public void afterPropertiesSet() {
        // 数据源校验和合并
        log.info("开始像Spring数据源路由器装载数据源");

        Map<Object, Object> allDataSources = new HashMap<>();
        allDataSources.putAll(masterDataSouces);
        if(null != slaveDataSources && 0 != slaveDataSources.size()) {
            allDataSources.putAll(slaveDataSources);
        }

        super.setTargetDataSources(allDataSources);
        super.afterPropertiesSet();

        log.info("已经完成Spring数据源路由器数据装载");
    }

    /**
     * 设置slave数据源
     * @param slaveDataSources
     */
    public void setSlaveDataSources(Map<String, DataSource> slaveDataSources) {
        if(null == slaveDataSources || 0 == slaveDataSources.size()) { // 从库数据源可以为空
            return;
        }
        log.info("可选的slave数据源:{}", slaveDataSources.keySet());
        this.slaveDataSources = slaveDataSources;
        this.slaveDataSourcesKeys = new ArrayList<>();
        for (Map.Entry<String, DataSource> entry : slaveDataSources.entrySet()) {
            slaveDataSourcesKeys.add(entry.getKey());
        }
    }

    /**
     * 设置master数据源
     * @param masterDataSouces
     */
    public void setMasterDataSouces(Map<String, DataSource> masterDataSouces) {
        if(null == masterDataSouces) {
            throw new IllegalArgumentException("主库数据源不能为空");
        }
        log.info("可选的master数据源:{}", masterDataSouces.keySet());
        this.masterDataSouces = masterDataSouces;
        this.masterDataSourcesKeys = new ArrayList<>();
        for (Map.Entry<String, DataSource> entry : masterDataSouces.entrySet()) {
            masterDataSourcesKeys.add(entry.getKey());
        }
    }

    /**
     * 标记从库
     */
    public void markSlave() {
        if(dataSourceHolder.get() != null) {
            throw new IllegalArgumentException("当前已有选取的数据源,不允许覆盖,已选取的数据源key: " + dataSourceHolder.get());
        }
        String dataSourcekey = selectFromSlave();
        setDataSource(dataSourcekey);
    }

    /**
     * 标记主库
     */
    public void markMaster() {
        if(dataSourceHolder.get() != null) {
            throw new IllegalArgumentException("当前已有选取的数据源,不允许覆盖,已选取的数据源key: " + dataSourceHolder.get());
        }
        String dataSourceKey = selectFromMaster();
        setDataSource(dataSourceKey);
    }

    public void markRemove() {
        dataSourceHolder.remove();
    }

    /**
     * 是否已绑定数据源
     * @return
     */
    public boolean hasBindedDataSource() {
        return dataSourceHolder.get() != null;
    }


    /**
     * 随机选择slave数据源
     * @return
     */
    private String selectFromSlave() {
        if(null == slaveDataSources) {
            log.info("无可选的slave数据源, 自动切换到master选取数据源");

            return selectFromMaster();
        }
        return slaveDataSourcesKeys.get(RandomUtils.nextInt(slaveDataSourcesKeys.size()));
    }

    /**
     * 随机选择master数据源
     * @return
     */
    private String selectFromMaster() {
        return masterDataSourcesKeys.get(RandomUtils.nextInt(masterDataSourcesKeys.size()));

    }

    /**
     * 设置threadlocal数据源key
     * @param dataSourceKey
     */
    private void setDataSource(String dataSourceKey) {
        log.info("set datasource keys:{}", dataSourceKey);
        dataSourceHolder.set(dataSourceKey);
    }


}
