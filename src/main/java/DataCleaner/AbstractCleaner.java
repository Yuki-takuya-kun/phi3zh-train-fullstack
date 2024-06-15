package DataCleaner;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.BlockingDeque;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

abstract class AbstractCleaner<T> {

    private volatile boolean gettingData = true;
    protected int maxDequeSize = Integer.MAX_VALUE;
    protected volatile BlockingDeque<T> data;

    // use queue to process element
    public void clean(){
        this.data = new LinkedBlockingDeque<>(maxDequeSize);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(() -> getDataThread());
        executorService.submit(() -> cleanAndSaveDataThread());
        executorService.shutdown();
    }

    protected abstract T cleanElement(T element);


    /**
     * this function is going the preprocess the dataset
     */
    protected abstract void getElements();

    /**
     * this function is going to process each element after clean
     * @param element the element that after clean
     */
    protected abstract void saveElement(T element);

    /**
     * the thread function that getting data
     */
    private void getDataThread(){
        this.gettingData = true;
        getElements();
        this.gettingData = false;
    }


    private void cleanAndSaveDataThread(){
        while (gettingData || !this.data.isEmpty()){
            if (!this.data.isEmpty()){
                try{
                    saveElement(cleanElement(this.data.take()));
                } catch (InterruptedException e){
                    Thread.currentThread().interrupt();
                }
            }
        }

    }
}
