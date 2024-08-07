package phi3zh.datacleaner;

import java.io.Serializable;
import java.util.Collection;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

abstract class AbstractCleaner<T> implements Serializable {

    protected boolean parallel;
    protected int maxDequeSize = Integer.MAX_VALUE;
    protected int maxRetry = 5;
    protected boolean end = true;

    // use queue to process element
    public void clean(){
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        if (this.parallel){
            executorService.submit(() -> getDataThread());
            executorService.submit(() -> cleanAndSaveDataThread());
            executorService.shutdown();
        } else {
            getDataThread();
            cleanAndSaveDataThread();
        }

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
        produceElements();
    }


    private void cleanAndSaveDataThread(){
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
