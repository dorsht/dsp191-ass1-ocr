import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.codec.binary.Base64;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Manager {
    static String bucketName;
    static String localAppToManagerQueueName;
    private static String arn;
    private static String keyName;
    private static String securityGroup;
    private static String workerAMI;
    private static String managerAMI;
    static AtomicInteger runningThreads;

    public static void main(String[] args) {
        try {
            System.out.println("Creating S3 Client...");
            final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .build();
            System.out.println("Creating SQSAsync Client...");
            final AmazonSQSAsync sqs = AmazonSQSAsyncClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .build();
            System.out.println("Creating EC2 Client...");
            final AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .build();

            bucketName = args[0];
            localAppToManagerQueueName = args[1];
            arn = args[2];
            keyName = args[3];
            securityGroup = args[4];
            workerAMI = args[5];
            managerAMI = args[6];

            runningThreads = new AtomicInteger(0);

            String localAppToManagerQueueUrl = sqs.getQueueUrl(localAppToManagerQueueName).getQueueUrl();

            System.out.println("Receiving new task message");
            serve(localAppToManagerQueueUrl, ec2, s3, sqs);

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }

    }

    @SuppressWarnings("InfiniteLoopStatement")
    private static void serve(String queue_url, AmazonEC2 ec2, AmazonS3 s3, AmazonSQSAsync sqs) {
        List<Message> messages;
        String msgBody;
        String[] parsed;
        int poolSize = 10;
        ExecutorService Executor = Executors.newFixedThreadPool(poolSize);
        while (true){
            try {
                messages = sqs.receiveMessageAsync(queue_url).get().getMessages();
                for (Message msg : messages){
                    msgBody = msg.getBody();
                    if (msgBody.contains("new task")) {
                        if (runningThreads.get() == poolSize){
                            createNewInstances(ec2, createManagerBashScript(), 1, false);
                        }
                        else{
                            System.out.println("Received message");
                            sqs.deleteMessage(queue_url, msg.getReceiptHandle());
                            parsed = msgBody.split(" ");
                            Runnable task = new TaskExecutor(parsed[4], parsed[3], Integer.parseInt(parsed[2]), ec2, s3, sqs);
                            Executor.submit(task);
                            runningThreads.incrementAndGet();
                        }
                    }
                }
                System.out.println("Looping");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Caught Interrupted Exception: " + e);

            } catch (ExecutionException e) {
                System.out.println("Caught Execution Exception: " + e);
            }
        }
    }

    static void sendDoneMessage(AmazonSQSAsync sqs, String localAppToManagerQueueUrl, String uuid) {
        sqs.sendMessageAsync(new SendMessageRequest()
                .withQueueUrl(localAppToManagerQueueUrl)
                .withMessageBody("done task "+ uuid)
                .withDelaySeconds(5));
    }

    static void waitForWorkersToProcessMessages(AmazonSQSAsync sqs, String managerToWorkerQueueUrl) throws InterruptedException {
        Thread.sleep(10000);
        boolean secondTry = false;
        while (true){
            GetQueueAttributesResult getQueueAttributesResult = sqs.getQueueAttributes(
                    new GetQueueAttributesRequest(managerToWorkerQueueUrl,
                            Collections.singletonList("ApproximateNumberOfMessages")));
            int size = Integer.parseInt(getQueueAttributesResult.getAttributes().values().iterator().next());
            System.out.println("size = " + size);
            if (size == 0){
                if (!secondTry){
                    secondTry = true;
                }
                else{
                    System.out.println();
                    break;
                }
            }
            //System.out.print(".");
            Thread.sleep(1000);
        }
    }

    static long sendNewImageTasksToQueue(AmazonSQSAsync sqs, BufferedReader reader, String queueUrl) throws IOException {
        long count = 0;
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
            count++;
            sqs.sendMessageAsync(new SendMessageRequest()
                    .withQueueUrl(queueUrl)
                    .withMessageBody("new image task " + line)
                    .withDelaySeconds(5));
        }
        return count;
    }

    static String createSQSQueue(AmazonSQSAsync sqs, String queueName) {
        CreateQueueRequest create_request = new CreateQueueRequest(queueName)
                .addAttributesEntry("ReceiveMessageWaitTimeSeconds", "20");
        try {
            sqs.createQueue(create_request);
        }
        catch (AmazonSQSException e) {
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }
        return sqs.getQueueUrl(queueName).getQueueUrl();
    }

    static String createWorkerBashScript(String managerToWorkerQueueName, String workerToManagerQueueName,
                                         String errorsQueueName) {
        String userData = "";
        userData = userData + "#cloud-boothook\n";
        userData = userData + "#!/bin/bash\n";
        userData = userData + "sudo java -jar /home/ubuntu/Worker/Worker-1.0.jar " + managerToWorkerQueueName + " " +
        workerToManagerQueueName + " " + errorsQueueName;

        return encodeBase64(userData);
    }

    private static String createManagerBashScript() {
        String userData = "";
        userData = userData + "#cloud-boothook\n";
        userData = userData + "#!/bin/bash\n";
        userData = userData + "sudo java -jar /home/ubuntu/Manager/Manager-1.0.jar " + bucketName + " " +
                localAppToManagerQueueName + " " + arn + " " + keyName + " " + securityGroup;
        return encodeBase64(userData);
    }

    static List<Instance> createNewInstances(AmazonEC2 ec2, String userData, int upper_limit, boolean isWorker){
        int lower_limit = 1;
        String ami;
        if (isWorker){
            if (upper_limit > 20){
                upper_limit = 20;
            }
            lower_limit = upper_limit - 10;
            if (lower_limit < 1){
                lower_limit = 1;
            }
            ami = workerAMI;
        }
        else {
            ami = managerAMI;
        }

        // TODO change ami's

        RunInstancesRequest runInstancesRequest = new RunInstancesRequest()
                .withImageId(ami)
                .withInstanceType(InstanceType.T2Medium)
                .withMinCount(lower_limit)
                .withMaxCount(upper_limit)
                .withKeyName(keyName)
                .withSecurityGroupIds(securityGroup)
                .withUserData(userData)
                .withIamInstanceProfile(new IamInstanceProfileSpecification().withArn(arn));
        //System.out.println("Done");
        return ec2.runInstances(runInstancesRequest).getReservation().getInstances();
    }

    private static String encodeBase64 (String input){
        String encoded = null;
        try {
            encoded = new String(Base64.encodeBase64(input.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return encoded;
    }

    private static String decodeBase64 (String input){
        String decoded = null;
        try {
            decoded = new String(Base64.decodeBase64(input.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return decoded;
    }

    static File createHtmlFile(AmazonSQSAsync sqs, String managerToWorkerQueueUrl) throws IOException {
        File file = File.createTempFile("output", ".html");
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        List<Message> messages;
        bw.write("<html>\n\t");
        bw.write("<head>\n\t\t");
        bw.write("<title>OCR</title>\n\t");
        bw.write("</head>\n");
        bw.write("<body>\n\t");
        String brnt = "<br/>\n\t";
        while (true){
            try {
                String decoded, answer;
                String [] parsed;
                messages = sqs.receiveMessageAsync(managerToWorkerQueueUrl).get().getMessages();
                for (Message msg : messages) {
                    sqs.deleteMessageAsync(managerToWorkerQueueUrl, msg.getReceiptHandle());
                    decoded = decodeBase64(msg.getBody());
                    if (decoded != null) {
                        parsed = decoded.split("\n");
                        answer = "<p>\n\t<img src=\""+parsed[0]+"\">"+brnt;
                        for (int i = 1; i < parsed.length; i++){
                            answer = answer.concat(parsed[i]+brnt);
                        }
                        answer = answer.concat("</p>\n");
                        bw.write(answer);
                    }
                }
                GetQueueAttributesResult getQueueAttributesResult = sqs.getQueueAttributes(
                        new GetQueueAttributesRequest(managerToWorkerQueueUrl,
                                Collections.singletonList("ApproximateNumberOfMessages")));
                int size = Integer.parseInt(getQueueAttributesResult.getAttributes().values().iterator().next());
                if (size == 0) {
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        bw.write("\t</body>\n");
        bw.write("</html>");
        bw.close();
        return file;
    }
}