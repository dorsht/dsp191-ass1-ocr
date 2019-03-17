import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.*;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.codec.binary.Base64;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;


public class LocalApplication {
    private static List<String> running_instance_ids = new LinkedList<String>();
    private static List<String> stopped_instance_ids = new LinkedList<String>();
    private static List<String> pending_instance_ids = new LinkedList<String>();
    private static List<Reservation> reservationList = null;
    private static String bucketName, queueName, arn, keyName, securityGroup, workerAMI, managerAMI;

    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Please provide input file name and number of images per worker," +
                    " example: \n" + "java -jar LocalApplication.jar images.txt 7");
        }
        String imagesFile = args[0];
        int n = Integer.parseInt(args[1]);
        if (n < 1) {
            throw new IllegalArgumentException("n must be greater than 0");
        }
        final AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
        reservationList = ec2.describeInstances().getReservations();
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
        final AmazonSQSAsync sqs = AmazonSQSAsyncClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();
        try {
            readUserInfo();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String uuid = "" + UUID.randomUUID(),
                outputFile = "output" + uuid + ".html";

        workerAMI  = "ami-0c1e39d62d70ac9b2";
        managerAMI = "ami-0434ee2b2075695fc";
        try {
            System.out.println("Creating bucket: " + bucketName);
            s3.createBucket(bucketName);

            System.out.println("Uploading " + imagesFile + " to s3 bucket...");
            s3.putObject(new PutObjectRequest(bucketName, imagesFile, new File(imagesFile)));

            System.out.println("Starting instance");
            startInstance(ec2);

            System.out.print("Waiting for instance to run...");
            waitForInstanceToRun(ec2);

            System.out.println("Creating Queue...");
            createQueue(queueName, sqs);

            System.out.println("Sending new task message to Queue...");
            String queueUrl = sendMessage(sqs, queueName, imagesFile, n, uuid);

            System.out.print("Waiting for done task message...");
            waitForDoneTaskMessage(queueUrl, sqs, uuid);

            System.out.println("Downloading HTML from bucket...");
            downloadHtmlFileFromBucket(s3, outputFile, uuid);

            System.out.println("Exiting");

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with Amazon, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());

        } catch (IOException ioe) {
            System.out.println("Caught IO Exception: " + ioe.getMessage());
        }
    }

    private static void createNewInstance(AmazonEC2 ec2, String userData) {
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest()
                .withImageId(managerAMI)
                .withInstanceType(InstanceType.T2Medium)
                .withMinCount(1)
                .withMaxCount(1)
                .withKeyName(keyName)
                .withSecurityGroupIds(securityGroup)
                .withUserData(userData)
                .withIamInstanceProfile(new IamInstanceProfileSpecification().withArn(arn));
        RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);
        running_instance_ids.add(runInstancesResult.getReservation().getInstances().get(0).getInstanceId());
    }

    private static String createBashScript() {
        String userData = "";
        userData = userData + "#cloud-boothook\n";
        userData = userData + "#!/bin/bash\n";
        userData = userData + "sudo java -jar /home/ubuntu/Manager/Manager-1.0.jar " + bucketName + " " +
                queueName + " " + arn + " " + keyName + " " + securityGroup + " " + workerAMI + " " + managerAMI;
        String base64UserData = null;
        try {
            base64UserData = new String(Base64.encodeBase64(userData.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return base64UserData;
    }

    private static void waitForInstanceToRun(AmazonEC2 ec2) {
        boolean notRunning = true;
        //List<Reservation> reservationList;
        int dotCount = 0;
        while (notRunning) {
            reservationList = ec2.describeInstances().getReservations();
            for (Reservation reservation : reservationList) {
                List<Instance> instanceList = reservation.getInstances();
                for (Instance instance : instanceList) {
                    if (instance.getInstanceId().equals(running_instance_ids.get(0))) {
                        if (instance.getState().getName().equals("pending")) {
                            System.out.print(".");
                            dotCount++;
                            if (dotCount == 15) {
                                dotCount = 0;
                                System.out.println();
                            }
                            break;
                        } else if (instance.getState().getName().equals("running")) {
                            notRunning = false;
                            System.out.println();
                        } else {
                            throw new AmazonServiceException("instance failed to run");
                        }
                    }
                }
            }
        }
        System.out.println("Instance is running");
    }

    private static void startInstance(AmazonEC2 ec2) {
        if (reservationList.size() == 0) {
            System.out.println("No instance found, Creating one");
            createNewInstance(ec2, createBashScript());
            ec2.rebootInstances(
                    new RebootInstancesRequest()
                            .withInstanceIds(running_instance_ids.get(0))
            );
        } else {
            for (Reservation reservation : reservationList) {
                List<Instance> instanceList = reservation.getInstances();
                for (Instance instance : instanceList) {
                    String status = instance.getState().getName();
                    String imageId = instance.getImageId();
                    if (imageId.equals(managerAMI)) {
                        if (status.equals("running")) {
                            running_instance_ids.add(instance.getInstanceId());
                        } else if (status.equals("stopped")) {
                            stopped_instance_ids.add(instance.getInstanceId());
                        } else if (status.equals("pending")) {
                            pending_instance_ids.add(instance.getInstanceId());
                        }
                    }
                }
            }
            if (running_instance_ids.size() == 0) {
                if (pending_instance_ids.size() == 0) {
                    if (stopped_instance_ids.size() == 0) {
                        System.out.println("No instance found, Creating one");
                        createNewInstance(ec2, createBashScript());
                        ec2.rebootInstances(
                                new RebootInstancesRequest()
                                        .withInstanceIds(running_instance_ids.get(0))
                        );
                    } else {
                        System.out.println("instance found, Starting it");
                        String id = stopped_instance_ids.get(0);
                        StartInstancesRequest startInstancesRequest = new StartInstancesRequest()
                                .withInstanceIds(id);

                        ec2.startInstances(startInstancesRequest);
                        running_instance_ids.add(id);
                    }
                } else {
                    running_instance_ids.add(pending_instance_ids.get(0));
                }
            }
        }
    }

    private static void createQueue(String queueName, AmazonSQS sqs) {
        CreateQueueRequest create_request = new CreateQueueRequest(queueName)
                .addAttributesEntry("ReceiveMessageWaitTimeSeconds", "5"); // maybe not good
        try {
            sqs.createQueue(create_request);

        } catch (AmazonSQSException e) {
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }
    }

    private static void downloadHtmlFileFromBucket(AmazonS3 s3, String outputFile, String uuid) throws IOException {
        S3Object o = s3.getObject(bucketName, "output" + uuid + ".html");
        S3ObjectInputStream s3is = o.getObjectContent();
        FileOutputStream fos = new FileOutputStream(new File(outputFile));
        byte[] read_buf = new byte[1024];
        int read_len;
        while ((read_len = s3is.read(read_buf)) > 0) {
            fos.write(read_buf, 0, read_len);
        }
        s3is.close();
        fos.close();
    }

    private static void waitForDoneTaskMessage(String queueUrl, AmazonSQS sqs, String uuid) {
        List<Message> messages;
        int dotCount = 0;
        boolean received = false;
        while (!received) {
            messages = sqs.receiveMessage(queueUrl).getMessages();
            int size = messages.size();
            if (size != 0) {
                for (Message msg : messages) {
                    if (msg.getBody().equals("done task " + uuid)) {
                        sqs.deleteMessage(queueUrl, msg.getReceiptHandle());
                        System.out.println();
                        received = true;
                    }
                }
                dotCount++;
                System.out.print(".");
                if (dotCount == 30) {
                    dotCount = 0;
                    System.out.println();
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Caught Interrupted Exception = " + e);
            }
        }
    }

    private static String sendMessage(AmazonSQS sqs, String queueName, String imagesFile, int n, String uuid) {
        String queue_url = sqs.getQueueUrl(queueName).getQueueUrl();
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queue_url)
                .withMessageBody("new task " + n + " " + imagesFile + " " + uuid)
                .withDelaySeconds(5);
        sqs.sendMessage(send_msg_request);
        return queue_url;
    }

    /** Assumption: The user used the userInfo file correctly - The first row is the bucket name, the second
           one is the queue name, the third one is the role arn, etc.
    **/
    private static void readUserInfo () throws IOException {
        File file = new File("./userInfo.txt");

        BufferedReader br = new BufferedReader(new FileReader(file));

        bucketName = br.readLine();
        queueName = br.readLine();
        arn = br.readLine();
        keyName = br.readLine();
        securityGroup = br.readLine();

        br.close();
    }
}