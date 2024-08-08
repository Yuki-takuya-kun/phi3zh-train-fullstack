package phi3zh.datacleaner;

import phi3zh.common.annotations.network.ExpBackoff;

import java.io.Serializable;
import java.util.Collection;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

abstract class AbstractCleaner<T> implements Serializable {

    protected boolean parallel;
    protected int maxRetry = 5;
    protected boolean end = false;

    // use queue to process element
    public void clean(){
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        if (this.parallel){
            executorService.submit(() -> produceElements());
            executorService.submit(() -> consumeElements());
            executorService.shutdown();
        } else {
            produceElements();
            consumeElements();
        }

    }

    /**
     * this function is going the preprocess the dataset
     */
    protected abstract void produceElements();

    protected abstract void consumeElements();
}
