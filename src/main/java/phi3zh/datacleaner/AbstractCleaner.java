package phi3zh.datacleaner;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AbstractCleaner<T> implements Serializable{

    // use queue to process element
    public void clean(){
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);
        executorService.submit(() -> {
            try {
                produceElements();
            } finally {
                latch.countDown();
            }
        });
        executorService.submit(() -> {
            try {
                consumeElements();
            } finally {
                latch.countDown();
            }
        });
        executorService.shutdown();
        try {
            latch.await();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * this function is going the preprocess the dataset
     */
    protected abstract void produceElements();

    protected abstract void consumeElements();
}
