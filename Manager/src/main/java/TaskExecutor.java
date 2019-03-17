import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQSAsync;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class TaskExecutor implements Runnable {
    private String imagesFile, uuid;
    private int n;
    private AmazonEC2 ec2;
    private AmazonS3 s3;
    private AmazonSQSAsync sqs;


    TaskExecutor(String uuid, String imagesFile, int n, AmazonEC2 ec2, AmazonS3 s3, AmazonSQSAsync sqs) {
        this.imagesFile = imagesFile;
        this.uuid = uuid;
        this.n = n;
        this.ec2 = ec2;
        this.s3 = s3;
        this.sqs = sqs;
    }

    public void run() {
        try{
            System.out.println("Downloading " + imagesFile + " from s3 bucket");
            S3Object object = s3.getObject(new GetObjectRequest(Manager.bucketName, imagesFile));
            InputStream input = object.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            System.out.println("Create Manager to Workers queue...");
            String managerToWorkerQueueName = "dsp-ass1-191-vd-mgr-to-wrk-queue-dor" + UUID.randomUUID();
            String managerToWorkerQueueUrl = Manager.createSQSQueue(sqs, managerToWorkerQueueName);
            System.out.println("Creating Worker to Manager Queue");
            String workerToManagerQueueName = "dsp-ass1-191-vd-wrk-to-mgr-queue-dor" + UUID.randomUUID();
            String workerToManagerQueueUrl = Manager.createSQSQueue(sqs, workerToManagerQueueName);
            System.out.println("Creating Errors Queue");
            String errorsQueueName = "dsp-ass1-191-vd-errors-queue-dor" + UUID.randomUUID();
            Manager.createSQSQueue(sqs, errorsQueueName);
            System.out.println("Sending new image task messages to queue");
            long count = Manager.sendNewImageTasksToQueue(sqs, reader, managerToWorkerQueueUrl);
            int numOfWorkers = (int) (count/n);
            if (numOfWorkers == 0){
                numOfWorkers = 1;
            }
            List<String> instanceIds = new LinkedList<String>();
            System.out.println("Creating "+ numOfWorkers +" worker instances");
            List<Instance> instances = Manager.createNewInstances(ec2,
                    Manager.createWorkerBashScript(managerToWorkerQueueName, workerToManagerQueueName,
                            errorsQueueName),numOfWorkers, true);
            System.out.println("Done");
            for (Instance instance: instances){
                instanceIds.add(instance.getInstanceId());
            }
            System.out.println("Waiting for Workers to process all messages.");
            Manager.waitForWorkersToProcessMessages(sqs, managerToWorkerQueueUrl);
            System.out.println("Creating HTML file");
            File htmlFile = Manager.createHtmlFile(sqs, workerToManagerQueueUrl);
            System.out.print("Uploading HTML file to s3 bucket...");
            s3.putObject(new PutObjectRequest(Manager.bucketName, "output" + uuid + ".html",  htmlFile));
            System.out.println("Sending done task message to App to Manager Queue");
            Manager.sendDoneMessage(sqs, Manager.localAppToManagerQueueName, uuid);
            System.out.println("Terminating Worker instances...");
            ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(instanceIds));
            sqs.deleteQueue(managerToWorkerQueueName);
            sqs.deleteQueue(workerToManagerQueueName);
            sqs.deleteQueue(errorsQueueName);

            Manager.runningThreads.decrementAndGet();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}