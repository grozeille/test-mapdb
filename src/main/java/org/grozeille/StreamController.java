package org.grozeille;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.mapdb.*;
import org.springframework.core.ExceptionDepthComparator;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Controller
@Slf4j
public class StreamController {

    final Executor executor;

    final CompletionService<Boolean> completionService;

    final DB db;

    final Atomic.Long sessionCounter;

    public StreamController(){
        db = DBMaker
                .fileDB("/tmp/mapdb")
                .transactionEnable()
                .fileChannelEnable()
                .make();
        sessionCounter = db.atomicLong("sessionCounter").createOrOpen();
        executor = Executors.newFixedThreadPool(4);
        completionService = new ExecutorCompletionService<Boolean>(executor);
    }

    @RequestMapping("/")
    @ResponseBody
    String home() {
        return "Hello World!";
    }

    @RequestMapping("/stream")
    public SseEmitter stream(
            @RequestParam(value = "requestId", required = true) long requestId,
            HttpServletRequest request) throws IOException {

        String lastEventIdString = request.getHeader("Last-Event-ID");
        Integer lastEventIdParsed = null;
        if(lastEventIdString != null){
            try {
                lastEventIdParsed = Integer.parseInt(lastEventIdString);
            }catch (Exception e){
                log.warn("Invalid last event id:" +lastEventIdString, e);
            }
        }
        final Integer lastEventId = lastEventIdParsed;

        log.info("HTTP Session ID: "+ request.getSession().getId());

        // TODO: should generate a new session id if null...
        // final long sessionId = sessionCounter.incrementAndGet();
        final String channelName = "channel_"+requestId;


        if(!db.exists(channelName)) {
            // start feeding in background
            executor.execute(() -> {
                log.info("Start feeding for channel "+channelName);
                IndexTreeList<String> channel = db.indexTreeList(channelName, Serializer.STRING).createOrOpen();

                for(int cpt = 0; cpt < 1000000; cpt++) {
                    // go as fast as you can!
                    channel.add("{ \"msgId\": "+cpt+" }");
                    if(cpt % 500 == 0){
                        db.commit();
                    }
                }

                db.commit();
                log.info("Feeding terminated for channel "+channelName);
            });
        }

        final SseEmitter sseEmitter = new SseEmitter();
        final AtomicBoolean stopSse = new AtomicBoolean(false);
        sseEmitter.onTimeout(() -> {
            log.info("On Timeout");
            stopSse.set(true);
        });
        sseEmitter.onCompletion(() -> {
            log.info("On Completion");
            stopSse.set(true);
        });

        // start consuming
        executor.execute(() -> {
            IndexTreeList<String> channel = db.indexTreeList(channelName, Serializer.STRING).createOrOpen();

            int cpt = lastEventId == null ? 0 : lastEventId;
            log.info("Start consuming for channel "+channelName+" from event: "+cpt);

            ListIterator<String> it = channel.listIterator(cpt);
            while(it.hasNext()){
                if(stopSse.get()){
                    break;
                }
                String s = it.next();
                try {
                    SseEmitter.SseEventBuilder eventBuilder = SseEmitter.event();
                    eventBuilder.id(Integer.toString(cpt));
                    eventBuilder.data(s, MediaType.APPLICATION_JSON);

                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        log.warn("InterruptedException", e);
                    }

                    sseEmitter.send(eventBuilder);
                } catch (IOException e) {
                    log.error("unable to sent event", e);
                    sseEmitter.completeWithError(e);
                }
                cpt++;
            }
            sseEmitter.complete();
            log.info("Consuming terminated for channel "+channelName);
        });

        return sseEmitter;
    }
}
