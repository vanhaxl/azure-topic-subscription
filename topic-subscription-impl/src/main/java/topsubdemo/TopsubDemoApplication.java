package topsubdemo;

import com.google.gson.Gson;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import topsub.connection.TopSubFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;

@SpringBootApplication(scanBasePackages = {"topsub.connection", "topsub.config"})
public class TopsubDemoApplication implements CommandLineRunner {
    static final Gson GSON = new Gson();

    @Autowired
    TopSubFactory topSubFactory;
    TopicClient topicMovie;
    TopicClient topicMusic;
    List<SubscriptionClient> movieSubscriptions;
    List<SubscriptionClient> musicSubscriptions;

    public static void main(String[] args) {
        SpringApplication.run(TopsubDemoApplication.class, args);
    }

    public void init() throws ServiceBusException, InterruptedException {
        this.topicMovie = topSubFactory.getTopicMovie();
        this.topicMusic = topSubFactory.getTopicMusic();
        this.movieSubscriptions = topSubFactory.getSubscriptionsOfTopicMovie();
        this.musicSubscriptions = topSubFactory.getSubscriptionsOfTopicMusic();
    }

    @Override
    public void run(String... args) throws Exception {
        long start = System.currentTimeMillis();
        init();
        sendMessagesToTopic(topicMovie);
        //receiveSubscriptionMessage();
        long end = System.currentTimeMillis();
        System.out.println("Time to execute is: " + (end - start) / 1000 + " seconds");
        System.exit(1);
    }

    private void sendMessagesToTopic(TopicClient topicClient) throws Exception {

        System.out.printf("\nSending orders to topic.\n");

        List<CompletableFuture> futures = new ArrayList<>();
        for (int i = 0; i < 500000; i++) {
            CompletableFuture<Long> future = sendOrder(topicClient, new Order("blue", i, "low"));
            futures.add(future);
            if (i > 0 && i % 2000 == 0) {
                // wait for all future done
                checkSendingToTopic(futures);
                futures.clear();
            }
        }


        System.out.printf("All messages sent.\n");
    }

    public void checkSendingToTopic(List<CompletableFuture> futures) throws ExecutionException, InterruptedException {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        for (CompletableFuture<Long> future : futures) {
            System.out.println("future status sending to Topic: " + future.isDone() + " " + future.get().toString());
            if (!future.isDone()) {
                System.out.println("wait for sending to Topic complete");

                try {
                    future.get(10, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    private CompletableFuture<Long> sendOrder(TopicClient topicClient, Order order) throws Exception {

        IMessage message = new Message(GSON.toJson(order, Order.class).getBytes(UTF_8));
        message.setCorrelationId(order.getPriority());
        message.setLabel(order.getColor());
        message.setProperties(new HashMap<String, String>() {{
            put("Color", order.getColor());
            put("Quantity", Integer.toString(order.getQuantity()));
            put("Priority", order.getPriority());
        }});

        System.out.printf("Sent order with Color=%s, Quantity=%d, Priority=%s\n", order.getColor(), order.getQuantity(), order.getPriority());
        //return topicClient.sendAsync(message);
        Instant instant = Instant.now();
        Instant latterInstant = instant.plus(10, ChronoUnit.MINUTES);
        return topicClient.scheduleMessageAsync(message, latterInstant);
    }

    private void receiveSubscriptionMessage() throws ServiceBusException, InterruptedException {
        movieSubscriptions.get(0).registerMessageHandler(new MessageHandler(), new MessageHandlerOptions());
        //movieSubscriptions.get(1).registerMessageHandler(new MessageHandler(), new MessageHandlerOptions());

    }

    static class MessageHandler implements IMessageHandler {
        static int count = 0;

        public CompletableFuture<Void> onMessageAsync(IMessage message) {
            final String messageString = new String(message.getBody(), StandardCharsets.UTF_8);
            System.out.println("Message: " + ++count);
            System.out.println("Received message: " + messageString);
            return CompletableFuture.completedFuture(null);
        }

        public void notifyException(Throwable exception, ExceptionPhase phase) {
            System.out.println(phase + " encountered exception:" + exception.getMessage());
        }
    }
}

/*
CompletableFuture[] arr = {
                sendOrder(topicClient, new Order()),
                sendOrder(topicClient, new Order("blue", 5, "low")),
                sendOrder(topicClient, new Order("red", 10, "high")),
                sendOrder(topicClient, new Order("yellow", 5, "low")),
                sendOrder(topicClient, new Order("blue", 10, "low")),
                sendOrder(topicClient, new Order("blue", 5, "high")),
                sendOrder(topicClient, new Order("blue", 10, "low")),
                sendOrder(topicClient, new Order("red", 5, "low")),
                sendOrder(topicClient, new Order("red", 10, "low")),
                sendOrder(topicClient, new Order("red", 5, "low")),
                sendOrder(topicClient, new Order("yellow", 10, "high")),
                sendOrder(topicClient, new Order("yellow", 5, "low")),
                sendOrder(topicClient, new Order("yellow", 10, "low"))

        };
 */
