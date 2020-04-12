import javax.swing.*;
import java.awt.*;
import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileInputStream;
import java.io.IOException;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataproc.v1.*;
import com.google.cloud.storage.*;
import com.google.common.collect.Lists;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class Client{
    static JFrame frame;
    static JPanel mainPanel;
    static JTextArea responseArea;
    static JLabel statusLabel;
    static JobControllerClient jobControllerClient = null;
    static ClusterControllerClient clusterControllerClient = null;
//            ClusterControllerSettings.newBuilder().setEndpoint(myEndpoint).build();

    public static Job waitForJobCompletion(
            JobControllerClient jobControllerClient, String projectId, String region, String jobId) {
        while (true) {
            // Poll the service periodically until the Job is in a finished state.
            Job jobInfo = jobControllerClient.getJob(projectId, region, jobId);
            switch (jobInfo.getStatus().getState()) {
                case DONE:
                case CANCELLED:
                case ERROR:
                    return jobInfo;
                default:
                    try {
                        // Wait a second in between polling attempts.
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
            }
        }
    }

    public static void main(String[] args) {
        frame = new JFrame();
        frame.setTitle("Hadoop");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//        frame.setLayout(new FlowLayout());

        // mainPanel
        mainPanel = new JPanel();
        mainPanel.setLayout(new BoxLayout(mainPanel, BoxLayout.Y_AXIS));

        JButton invertedIndexButton = new JButton("Construct Inverted Index");
        invertedIndexButton.setAlignmentX(Component.CENTER_ALIGNMENT);
        mainPanel.add(invertedIndexButton);
        statusLabel = new JLabel("press button to run hadoop, this might take a while");
        statusLabel.setAlignmentX(Component.CENTER_ALIGNMENT);
        mainPanel.add(statusLabel);

        responseArea = new JTextArea("");
        JScrollPane sp = new JScrollPane(responseArea);
        responseArea.setLineWrap(true);
//        mainPanel.add(responseArea);
        mainPanel.add(sp);





        frame.add(mainPanel);
        frame.setSize(500, 500);
        frame.setVisible(true);

        invertedIndexButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent event) {
                statusLabel.setText("bad encoding at the beginning");
                String region = "us-west1";
                String clusterName = "cluster-diw26-1660";
                String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);
                String mainClass = "WordCount";
                String projectId = "ethereal-jigsaw-251011";
                JobControllerSettings jobControllerSettings = null;
                ClusterControllerSettings clusterControllerSettings = null;
                try {

                    clusterControllerSettings = ClusterControllerSettings.newBuilder().setEndpoint(myEndpoint).build();
                    jobControllerSettings = JobControllerSettings.newBuilder().setEndpoint(myEndpoint).build();
                    clusterControllerClient = ClusterControllerClient.create(clusterControllerSettings);
                    jobControllerClient = JobControllerClient.create(jobControllerSettings);
                    JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build();
                    HadoopJob hadoopJob = HadoopJob.newBuilder().setMainClass(mainClass)
                            .addJarFileUris("gs://dataproc-staging-us-west1-488178233660-a3cwhvbg/JAR/wordcount.jar")
                            .addArgs("gs://dataproc-staging-us-west1-488178233660-a3cwhvbg/Data")
                            .addArgs("gs://dataproc-staging-us-west1-488178233660-a3cwhvbg/Output")
                            .build();
                    Job job = Job.newBuilder().setPlacement(jobPlacement).setHadoopJob(hadoopJob).build();
                    Job request = jobControllerClient.submitJob(projectId, region, job);
                    String jobId = request.getReference().getJobId();
                    System.out.println(String.format("Submitted job \"%s\"", jobId));

                    CompletableFuture<Job> finishedJobFuture = CompletableFuture.supplyAsync(new Supplier<Job>() {
                        @Override
                        public Job get() {
                            return waitForJobCompletion(jobControllerClient, projectId, region, jobId);
                        }
                    });
                    int timeout = 10;
                    try {
                        Job jobInfo = finishedJobFuture.get(timeout, TimeUnit.MINUTES);
                        System.out.println(String.format("Job %s finished successfully.", jobId));

                        // Cloud Dataproc job output gets saved to a GCS bucket allocated to it.
                        Cluster clusterInfo = clusterControllerClient.getCluster(projectId, region, clusterName);
                        Storage storage = StorageOptions.getDefaultInstance().getService();
                        Blob blob = storage.get(
                                clusterInfo.getConfig().getConfigBucket(),"Output/part-r-00000");
                        System.out.println( String.format( "Job \"%s\" finished with state %s:\n%s", jobId, jobInfo.getStatus().getState(), new String(blob.getContent())));
                        responseArea.setText(new String(blob.getContent()));
                    } catch (TimeoutException timeoutException) {
                        System.err.println(
                                String.format("Job timed out after %d minutes: %s", timeout, timeoutException.getMessage()));
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                } catch (IOException ioException) {
                    System.out.println(ioException);
                    if (jobControllerClient != null) {
                        jobControllerClient.close();
                    }
                }

            }
        });


//        String region = "us-west1";
//        String clusterName = "cluster-diw26-1660";
//        String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);
//        String mainClass = "WordCount";
//        String projectId = "ethereal-jigsaw-251011";
//        JobControllerSettings jobControllerSettings = null;
//        ClusterControllerSettings clusterControllerSettings = null;
//        try {
//
//            clusterControllerSettings = ClusterControllerSettings.newBuilder().setEndpoint(myEndpoint).build();
//            jobControllerSettings = JobControllerSettings.newBuilder().setEndpoint(myEndpoint).build();
//            clusterControllerClient = ClusterControllerClient.create(clusterControllerSettings);
//            jobControllerClient = JobControllerClient.create(jobControllerSettings);
//            JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build();
//            HadoopJob hadoopJob = HadoopJob.newBuilder().setMainClass(mainClass)
//                    .addJarFileUris("gs://dataproc-staging-us-west1-488178233660-a3cwhvbg/JAR/wordcount.jar")
//                    .addArgs("gs://dataproc-staging-us-west1-488178233660-a3cwhvbg/Data")
//                    .addArgs("gs://dataproc-staging-us-west1-488178233660-a3cwhvbg/Output")
//                    .build();
//            Job job = Job.newBuilder().setPlacement(jobPlacement).setHadoopJob(hadoopJob).build();
//            Job request = jobControllerClient.submitJob(projectId, region, job);
//            String jobId = request.getReference().getJobId();
//            System.out.println(String.format("Submitted job \"%s\"", jobId));
//
////            CompletableFuture<Job> finishedJobFuture = CompletableFuture.supplyAsync(() -> waitForJobCompletion(jobControllerClient, projectId, region, jobId));
//            CompletableFuture<Job> finishedJobFuture = CompletableFuture.supplyAsync(new Supplier<Job>() {
//                @Override
//                public Job get() {
//                    return waitForJobCompletion(jobControllerClient, projectId, region, jobId);
//                }
//            });
//            int timeout = 10;
//            try {
//                Job jobInfo = finishedJobFuture.get(timeout, TimeUnit.MINUTES);
//                System.out.println(String.format("Job %s finished successfully.", jobId));
//
//                // Cloud Dataproc job output gets saved to a GCS bucket allocated to it.
//                Cluster clusterInfo = clusterControllerClient.getCluster(projectId, region, clusterName);
//                Storage storage = StorageOptions.getDefaultInstance().getService();
//                Blob blob = storage.get(
//                                clusterInfo.getConfig().getConfigBucket(),"Output/part-r-00000");
//                System.out.println( String.format( "Job \"%s\" finished with state %s:\n%s", jobId, jobInfo.getStatus().getState(), new String(blob.getContent())));
//                responseArea.setText(new String(blob.getContent()));
//            } catch (TimeoutException e) {
//                System.err.println(
//                        String.format("Job timed out after %d minutes: %s", timeout, e.getMessage()));
//            } catch (Exception e) {
//                System.out.println(e);
//            }
//        } catch (IOException e) {
//            System.out.println(e);
//            if (jobControllerClient != null) {
//                jobControllerClient.close();
//            }
//        }




    }

//    static void authExplicit(String jsonPath) throws IOException {
//        // You can specify a credential file by providing a path to GoogleCredentials.
//        // Otherwise credentials are read from the GOOGLE_APPLICATION_CREDENTIALS environment variable.
//        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath))
//                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
//        Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
//
//        System.out.println("Buckets:");
//        Page<Bucket> buckets = storage.list();
//        for (Bucket bucket : buckets.iterateAll()) {
//            System.out.println(bucket.toString());
//        }
//    }
//
//    public static void test() {
//        System.out.println("testing");
//    }
}
