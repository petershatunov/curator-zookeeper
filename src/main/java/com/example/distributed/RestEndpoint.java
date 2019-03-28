package com.example.distributed;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedValue;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.websocket.server.PathParam;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

@RestController
public class RestEndpoint {

    private CuratorFramework zk;
    private final Logger logger = Logger.getLogger(getClass().getCanonicalName());

    @PostConstruct
    private synchronized void connectToZk() {
	System.out.println("Connecting to ZK...");
	int sleepMsBetweenRetries = 100;
	int maxRetries = 3;
	RetryPolicy retryPolicy = new RetryNTimes(maxRetries, sleepMsBetweenRetries);
	zk = CuratorFrameworkFactory.newClient("127.0.0.1:2181", retryPolicy);
	zk.start();
    }

    @GetMapping(value = "/distr")
    public String performSomeLogic(@PathParam(value = "str") String str) throws Exception {

	performSharedLock(5500);
	performSharedVariable();

	return str;
    }

    private void performSharedLock(long millisec) throws Exception {
	InterProcessSemaphoreMutex sharedLock = new InterProcessSemaphoreMutex(zk, "/mutex/process/A");
	sharedLock.acquire();
	Thread.sleep(millisec);
	sharedLock.release();
    }

    private void performSharedVariable() throws Exception {
	SharedCount counter = new SharedCount(zk, "/counters/A", 0);
	counter.start();
	int count = counter.getCount();
	logger.info("count = " + count);
	counter.setCount(count + 1);
	counter.close();

	String xml = "xml";
	SharedValue sharedValue = new SharedValue(zk, "/value/A", xml.getBytes());
	sharedValue.start();
	logger.info("value = " + new String(sharedValue.getValue(), StandardCharsets.UTF_8));
	sharedValue.setValue((xml + counter.getCount()).getBytes());
	sharedValue.close();

    }

}
