package com.gospelye.routing;

import org.aspectj.lang.ProceedingJoinPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;

/**
 * Created by yewenhai on 15/10/30.
 */
public class DynamicDataSourceAop {

    private static final Logger log = LoggerFactory.getLogger(DynamicDataSourceAop.class);
    @Resource
    private DynamicDataSourceHolder dataSourceHolder;

    public Object doAroundMethod(ProceedingJoinPoint point) throws Throwable {
        Object response = null;

        String method = point.getSignature().getName();
        boolean hasBinded = false;

        try {
            hasBinded = dataSourceHolder.hasBindedDataSource();
            if(!hasBinded) {
                if (method.startsWith("query") || method.startsWith("select")) { // 查询走从库
                    dataSourceHolder.markSlave();
                } else { // 其他默认走主库
                    dataSourceHolder.markMaster();
                }
                log.info("aop数据库切换 method:{},datasource:{}", method, dataSourceHolder.determineCurrentLookupKey());
            }
            response = point.proceed();
        } finally {
            if(hasBinded) {
                dataSourceHolder.markRemove();
            }
        }

        return response;
    }
}
