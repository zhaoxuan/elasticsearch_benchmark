package com.baidu.dtrace;

/**
 * Created by john on 12/16/14.
 */

import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

public class EsBulkProcessor implements Listener {

    public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {

    }

    public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {

    }

    public void beforeBulk(long executionId, BulkRequest request) {

    }
}
