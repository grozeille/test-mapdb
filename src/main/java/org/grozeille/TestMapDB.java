package org.grozeille;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.IndexTreeList;
import org.mapdb.Serializer;

import java.util.ListIterator;
import java.util.concurrent.*;

public class TestMapDB {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        final DB db = DBMaker
                .fileDB("/tmp/mapdb")
                .fileChannelEnable()
                .make();

        final int max = 1000000;

        final String channelName = "request_01234";

        // for test, clear before
        IndexTreeList<String> channel = db.indexTreeList(channelName, Serializer.STRING).createOrOpen();
        channel.clear();

        Executor executor = Executors.newFixedThreadPool(4);
        CompletionService<Boolean> completionService = new ExecutorCompletionService<Boolean>(executor);

        // start feeding
        Future<Boolean> futureFeed = completionService.submit(() -> {
            IndexTreeList<String> channel1 = db.indexTreeList(channelName, Serializer.STRING).createOrOpen();


            for(int cpt = 0; cpt < max; cpt++) {
                channel1.add("{ msgId: "+cpt+" }");
            }

            db.commit();

            return true;
        });

        // start consuming
        Future<Boolean> futureConsume = completionService.submit(() -> {

            IndexTreeList<String> channel1 = db.indexTreeList(channelName, Serializer.STRING).createOrOpen();

            System.out.println("Read from beginning");

            // read from beginning
            ListIterator<String> it = channel1.listIterator(0);
            int cpt = 0;
            while(it.hasNext()){
                String s = it.next();
                System.out.println(s);
                cpt++;
                if(cpt >= 157){
                    System.out.println("Oops, crash...");
                    break;
                }
            }

            System.out.println("Read from last crash");

            // read from middle
            it = channel1.listIterator(157);
            while(it.hasNext()){
                String s = it.next();
                System.out.println(s);
            }

            return true;
        });

        // wait for end
        futureFeed.get();
        futureConsume.get();

    }
}
