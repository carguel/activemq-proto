/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Date;
import java.util.Formatter;
import java.util.Locale;


import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * A simple tool for consuming messages
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class ConsumerTool extends Thread implements MessageListener, ExceptionListener {

    private int received_messages = 0;
    private Thread startThread;
    private Date firstMessageDate = null;
    private int accumulatedSize = 0;
    private Date lastMessageDate = null;


    private boolean running;

    private Session session;
    private Destination destination;
    private MessageProducer replyProducer;

    private boolean pauseBeforeShutdown = false;
    private boolean verbose = true;
    private int maxiumMessages;
    private static int parallelThreads = 1;
    private String subject = "TOOL.DEFAULT";
    private boolean topic;
    private String user = ActiveMQConnection.DEFAULT_USER;
    private String password = ActiveMQConnection.DEFAULT_PASSWORD;
    private String url = ActiveMQConnection.DEFAULT_BROKER_URL;
    private boolean transacted;
    private boolean durable;
    private String clientId;
    private int ackMode = Session.AUTO_ACKNOWLEDGE;
    private String consumerName = "James";
    private long sleepTime;
    private long receiveTimeOut;

    public static void main(String[] args) {
        ArrayList<ConsumerTool> threads = new ArrayList();
        final ConsumerTool consumerTool = new ConsumerTool();
        String[] unknown = CommandLineSupport.setOptions(consumerTool, args);
        if (unknown.length > 0) {
            System.out.println("Unknown options: " + Arrays.toString(unknown));
            System.exit(-1);
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public synchronized void start() {

                try {
                System.out.println("Stopping");

                try {
                    Thread.sleep(1000l);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                double receivingTime = consumerTool.getReceivingTime();
                System.out.println("Exiting");
                int receivedMessage = consumerTool.getReceivedMessages();
                double receivedMegaByte = consumerTool.getMegaByteReceived();
                double messagePerSecond = consumerTool.getMessagePerSecond();
                double megaBytePersecond = consumerTool.getMegaBytePerSecond();
                String message = "\n [*] Stopping :  %d messages /  %.3f MB received in %.3f seconds ( %.3f msg/s / %.3f MB/s)";
                Formatter formatter = new Formatter(System.out);
                formatter.format(Locale.ENGLISH, message, receivedMessage, receivedMegaByte, receivingTime, messagePerSecond, megaBytePersecond);
                formatter.flush();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        consumerTool.showParameters();
        CommandLineSupport.setOptions(consumerTool, args);
        consumerTool.start();
        threads.add(consumerTool);

    }

    public void showParameters() {
        System.out.println("Connecting to URL: " + url);
        System.out.println("Consuming " + (topic ? "topic" : "queue") + ": " + subject);
        System.out.println("Using a " + (durable ? "durable" : "non-durable") + " subscription");
        System.out.println("Running " + parallelThreads + " parallel threads");
    }

    public void run() {
        try {
            running = true;

            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, url);
            Connection connection = connectionFactory.createConnection();
            if (durable && clientId != null && clientId.length() > 0 && !"null".equals(clientId)) {
                connection.setClientID(clientId);
            }
            connection.setExceptionListener(this);
            connection.start();

            session = connection.createSession(transacted, ackMode);
            if (topic) {
                destination = session.createTopic(subject);
            } else {
                destination = session.createQueue(subject);
            }

            replyProducer = session.createProducer(null);
            replyProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            MessageConsumer consumer = null;
            if (durable && topic) {
                consumer = session.createDurableSubscriber((Topic) destination, consumerName);
            } else {
                consumer = session.createConsumer(destination);
            }

            consumer.setMessageListener(this);

        } catch (Exception e) {
            System.out.println("[" + this.getName() + "] Caught: " + e);
            e.printStackTrace();
        }
    }

    public void onMessage(Message message) {
        try {

            if (firstMessageDate == null) {
                firstMessageDate = new Date();
            }

            TextMessage txtMsg = (TextMessage) message;
            String msg = txtMsg.getText();

            received_messages++;
            accumulatedSize += msg.length();
            lastMessageDate = new Date();

            if (received_messages % 1000 == 0) {
                System.out.println(" [x] " + received_messages + " messages received");
            }


            if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
                message.acknowledge();
            }

        } catch (JMSException e) {
            System.out.println("[" + this.getName() + "] Caught: " + e);
            e.printStackTrace();
        } finally {
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    public synchronized void onException(JMSException ex) {
        System.out.println("[" + this.getName() + "] JMS Exception occured.  Shutting down client.");
        running = false;
    }

    synchronized boolean isRunning() {
        return running;
    }

    protected void consumeMessagesAndClose(Connection connection, Session session, MessageConsumer consumer) throws JMSException,
            IOException {
        System.out.println("[" + this.getName() + "] We are about to wait until we consume: " + maxiumMessages
                + " message(s) then we will shutdown");

        for (int i = 0; i < maxiumMessages && isRunning();) {
            Message message = consumer.receive(1000);
            if (message != null) {
                i++;
                onMessage(message);
            }
        }
        System.out.println("[" + this.getName() + "] Closing connection");
        consumer.close();
        session.close();
        connection.close();
        if (pauseBeforeShutdown) {
            System.out.println("[" + this.getName() + "] Press return to shut down");
            System.in.read();
        }
    }

    protected void consumeMessagesAndClose(Connection connection, Session session, MessageConsumer consumer, long timeout)
            throws JMSException, IOException {
        System.out.println("[" + this.getName() + "] We will consume messages while they continue to be delivered within: " + timeout
                + " ms, and then we will shutdown");

        Message message;
        while ((message = consumer.receive(timeout)) != null) {
            onMessage(message);
        }

        System.out.println("[" + this.getName() + "] Closing connection");
        consumer.close();
        session.close();
        connection.close();
        if (pauseBeforeShutdown) {
            System.out.println("[" + this.getName() + "] Press return to shut down");
            System.in.read();
        }
    }

    public void setAckMode(String ackMode) {
        if ("CLIENT_ACKNOWLEDGE".equals(ackMode)) {
            this.ackMode = Session.CLIENT_ACKNOWLEDGE;
        }
        if ("AUTO_ACKNOWLEDGE".equals(ackMode)) {
            this.ackMode = Session.AUTO_ACKNOWLEDGE;
        }
        if ("DUPS_OK_ACKNOWLEDGE".equals(ackMode)) {
            this.ackMode = Session.DUPS_OK_ACKNOWLEDGE;
        }
        if ("SESSION_TRANSACTED".equals(ackMode)) {
            this.ackMode = Session.SESSION_TRANSACTED;
        }
    }

    public void setClientId(String clientID) {
        this.clientId = clientID;
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public void setMaxiumMessages(int maxiumMessages) {
        this.maxiumMessages = maxiumMessages;
    }

    public void setPauseBeforeShutdown(boolean pauseBeforeShutdown) {
        this.pauseBeforeShutdown = pauseBeforeShutdown;
    }

    public void setPassword(String pwd) {
        this.password = pwd;
    }

    public void setReceiveTimeOut(long receiveTimeOut) {
        this.receiveTimeOut = receiveTimeOut;
    }

    public void setSleepTime(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public void setParallelThreads(int parallelThreads) {
        if (parallelThreads < 1) {
            parallelThreads = 1;
        }
        this.parallelThreads = parallelThreads;
    }

    public void setTopic(boolean topic) {
        this.topic = topic;
    }

    public void setQueue(boolean queue) {
        this.topic = !queue;
    }

    public void setTransacted(boolean transacted) {
        this.transacted = transacted;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    protected double getMegaBytePerSecond() {
        System.out.println("toto");
        return  getMegaByteReceived() / getReceivingTime();
    }

    protected double getMessagePerSecond() {
        return (double)(getReceivedMessages() / getReceivingTime());
    }

    protected int getReceivedMessages() {
        return received_messages;
    }

    protected double getReceivingTime() {
        return (lastMessageDate.getTime() - firstMessageDate.getTime()) / 1000.0;
    }

    protected double getMegaByteReceived() {
        return accumulatedSize / 1000.0 / 1000.0;
    }

}
