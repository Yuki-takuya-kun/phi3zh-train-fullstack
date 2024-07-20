package datacleaner;

import java.io.Serializable;
import java.util.Collection;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

abstract class AbstractCleaner<T> implements Serializable {

    private volatile boolean gettingData = true;
    protected int maxDequeSize = Integer.MAX_VALUE;
    protected int maxRetry = 5;

    // use queue to process element
    public void clean(){
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(() -> getDataThread());
        executorService.submit(() -> cleanAndSaveDataThread());
        executorService.shutdown();
    }

    /**
     * this function is going the preprocess the dataset
     */
    protected abstract void produceElements();

    protected abstract Collection<T> consumeElements();

    protected abstract Collection<T> cleanElements(Collection<T> element);

    /**
     * this function is going to process each element after clean
     * @param element the element that after clean
     */
    protected abstract void saveElements(Collection<T> element);

    /**
     * the thread function that getting data
     */
    private void getDataThread(){
        this.gettingData = true;
        System.out.println("produce elements");
        produceElements();
    }


    private void cleanAndSaveDataThread(){
        System.out.println("begin clean data");
        boolean end = false;
        int retry = 0;
        while (!end){
            try {
                saveElements(cleanElements(consumeElements()));
            } catch (Exception e){
                retry ++;
                if (retry >= this.maxRetry){
                    end = true;
                }
            }
        }
        //System.out.println("end clean data");
    }
}
