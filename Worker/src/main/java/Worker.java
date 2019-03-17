import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.codec.binary.Base64;

import java.io.*;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Worker {
    public static void main(String[] args){


        final AmazonSQSAsync sqs = AmazonSQSAsyncClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        String managerToWorkersQueueName = args[0];
        String managerToWorkersQueueUrl = sqs.getQueueUrl(managerToWorkersQueueName).getQueueUrl();
        String workersToManagerQueueName = args[1];
        String workersToManagerQueueUrl = sqs.getQueueUrl(workersToManagerQueueName).getQueueUrl();
        String errorsQueueName = args[2];
        String errorsQueueUrl = sqs.getQueueUrl(errorsQueueName).getQueueUrl();

        while(true){

            System.out.println("Receiving message...");
            List<Message> message;
            try {
                message = sqs.receiveMessageAsync(new ReceiveMessageRequest(managerToWorkersQueueUrl)
                        .withMaxNumberOfMessages(1))
                        .get()
                        .getMessages();
            } catch (InterruptedException e) {
                sendMessage(encodeBase64(e.toString()), errorsQueueUrl, sqs);
                continue;
            } catch (ExecutionException e) {
                sendMessage(encodeBase64(e.toString()), errorsQueueUrl, sqs);
                continue;
            }

            if (message.size() == 1){


                    String msg = message.get(0).getBody();
                    String[] spaceParse = msg.split(" ");
                    String fileUrl = spaceParse[3];
                    System.out.println("Running \"wget "+ fileUrl +"\" linux terminal command");
                try {
                    runWgetCommand(fileUrl);
                } catch (IOException e) {
                    sendMessage(encodeBase64(e.toString()), errorsQueueUrl, sqs);
                    continue;
                } catch (InterruptedException e) {
                    sendMessage(encodeBase64(e.toString()), errorsQueueUrl, sqs);
                    continue;
                }

                String[] slashParse = fileUrl.split("/");
                    String fileName = slashParse[slashParse.length-1];
                    System.out.println("Running \"tesseract "+ fileName +"\" linux terminal command");
                try {
                    runTesseractCmd(fileName);
                } catch (IOException e) {
                    sendMessage(encodeBase64(e.toString()), errorsQueueUrl, sqs);
                    continue;
                } catch (InterruptedException e) {
                    sendMessage(encodeBase64(e.toString()), errorsQueueUrl, sqs);
                    continue;
                }

                System.out.println("Creating message to Manager...");
                String answer;
                try {
                    answer = createMessage(fileUrl);
                } catch (IOException e) {
                    sendMessage(encodeBase64(e.toString()), errorsQueueUrl, sqs);
                    continue;
                }


                System.out.println("Sending message to Worker to Manager queue...");
                sendMessage(answer, workersToManagerQueueUrl, sqs);

                sqs.deleteMessage(managerToWorkersQueueUrl, message.get(0).getReceiptHandle());
            }
            else{
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    sendMessage(encodeBase64(e.toString()), errorsQueueUrl, sqs);
                }
            }
        }
    }
    private static void sendMessage (String message, String queueUrl, AmazonSQSAsync sqs){
        sqs.sendMessageAsync(new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(message)
                .withDelaySeconds(5));
    }
    private static String encodeBase64 (String input){
        String base64 = null;
        try {
            base64 = new String( Base64.encodeBase64(input.getBytes( "UTF-8" )), "UTF-8" );
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return base64;
    }
    private static String createMessage(String fileUrl) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File("output.txt")));
        String answer = fileUrl+"\n";

        String line = br.readLine();

        while(line!=null){
            answer = answer.concat(line+"\n");
            line = br.readLine();
        }
        br.close();
        return encodeBase64(answer);
    }

    private static void runTesseractCmd(String fileName) throws IOException, InterruptedException {
        String[] runTesseractCmd = new String[] {"sudo", "tesseract", fileName, "output"};
        Process runTesseract = new ProcessBuilder(runTesseractCmd).start();
        runTesseract.waitFor();
    }

    private static void runWgetCommand(String file) throws IOException, InterruptedException {
        String[] cmdArray = new String[] {"sudo", "wget", file};
        Process wget = new ProcessBuilder(cmdArray).start();
        wget.waitFor();
    }
}
