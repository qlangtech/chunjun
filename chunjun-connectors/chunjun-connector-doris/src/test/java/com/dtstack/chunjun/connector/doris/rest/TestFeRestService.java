package com.dtstack.chunjun.connector.doris.rest;

import junit.framework.TestCase;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/14
 */
public class TestFeRestService extends TestCase {

    public void testGetBeNodes() throws Exception{
        System.out.println(  FeRestService.get("http://192.168.28.200:8030/rest/v1/system?path=//backends","root",""));


        System.out.println(  FeRestService.get("http://192.168.28.200:8030/api/backends?is_alive=true","root",""));
    }
}
