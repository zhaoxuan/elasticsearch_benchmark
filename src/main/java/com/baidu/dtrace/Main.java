package com.baidu.dtrace;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class Main implements Runnable {
    private int bulkNumber;
    private String indexName;

    public Main(String[] args) {
        this.indexName = args[2];
        this.bulkNumber = Integer.parseInt(args[1]);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("java -jar elasticsearch-1.0-SNAPSHOT-jar-with-dependencies.jar threadNum bulkNum indexName");
        }
        int threadNumber = Integer.parseInt(args[0]);


        ExecutorService executors = Executors.newFixedThreadPool(threadNumber);
        for (int i = 0; i < threadNumber; i++) {
            executors.submit(new Main(args));
        }

    }

    public void run() {
        String[] hosts = {"10.202.86.35"};

        insertEs(hosts, this.bulkNumber, this.indexName);
    }

    public void insertEs(String[] hosts, int bulkNumber, String indexName) {
        EsClient client = new EsClient(hosts, 8300, "dtrace_test");
        client.connectEs();

        client.setIndexName(indexName);
        client.setTypeName("benchmark");
        client.bulkApi(bulkNumber);

        client.closeEs();
    }

}
