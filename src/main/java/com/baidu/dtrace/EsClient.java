package com.baidu.dtrace;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.Iterator;
import java.util.Map;


/**
 * Created by john on 12/11/14.
 */
public class EsClient {
    protected String[] hosts;
    protected int port;
    protected String esCluster;
    protected String indexName;
    protected String typeName;
    private Client esClient;

    public EsClient(String[] hosts, int port, String esCluster) {
        this.hosts = hosts;
        this.port = port;
        this.esCluster = esCluster;
    }

    public void connectEs() {
        Settings settings = ImmutableSettings.settingsBuilder()
                            .put("action.bulk.compress", true)
                            .put("network.tcp.send_buffer_size", 131072)
                            .put("network.tcp.receive_buffer_size", 8192)
                            .put("transport.tcp.compress", true)
                            .put("cluster.name", this.esCluster).build();

        TransportClient client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(hosts[0], this.port));

        NodesInfoResponse nodesInfo;
        nodesInfo = client.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet();
        Map<String, NodeInfo> nodesMap = nodesInfo.getNodesMap();

        Iterator<String> its;
        its = nodesMap.keySet().iterator();
        while (its.hasNext()) {
            String key = its.next();
            String name = nodesMap.get(key).getHostname();
            client.addTransportAddress(new InetSocketTransportAddress(name, this.port));

        }

//        TransportClient transportClient = new TransportClient(settings);
//
//        for (int i = 0; i < hosts.length; i++) {
//            transportClient.addTransportAddress(new InetSocketTransportAddress(hosts[i], this.port));
//        }
        this.esClient = client;

    }

    public void setIndexName(String name) {this.indexName = name;}

    public void setTypeName(String name) {this.typeName = name;}

    public void closeEs() {this.esClient.close();}

    public void bulkApi(int bulkNumber) {

        BulkProcessor bulkProcess = BulkProcessor.builder(this.esClient, new EsBulkProcessor())
                .setBulkActions(bulkNumber)
                .setConcurrentRequests(3).build();

        String document = "{\"module\" : \"zipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.ucloginzipkin.example.uclogin\", \"trace_id\" : 60911064754696185, \"uclogin.response.status\" : \"success&igc=12817xz70948aba96d133&word=xu76&appid=0success&igc=12817xz70948aba96d133&word=xu76&appid=0\", \"uclogin.response.body\" : \"中国\", \"page_view\" : \"1\", \"uclogin.request.url\" : \"application\\/controllers\\/Index.php\", \"service\" : \"zipkin.example.uclogin\", \"response_time\" : \"0\", \"event_time\" : 1418275258639, \"zipkin_time\" : 1418177204917, \"product\" : \"john\"}";

        while (true) {
            bulkProcess.add(new IndexRequest(indexName, typeName).source(document));
        }
    }
}
