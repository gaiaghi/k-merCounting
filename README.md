Project exam for the course "Scalable and Cloud Programming" (University of Bologna).

# Introduction
The project aims at developing a Scala + Spark application, implementing the k-mer counting algorithm by exploiting the map-reduce programming model, to later test its scalability in the cloud context.

K-mer counting is a genetic sequence analysis task, which counts the frequency of each k-mer in a given genetic sequence, where a k-mer is a substring of length **k**. There can be two types of counting:
*    *non-canonical counting*: every different sequence of characters is considered as such;
*    *canonical counting*: each sequence it considered equal to its reverse complement.

Given a genetic sequence, its reverse complement is the sequence in the opposite DNA strand (where the letters are reversed, A-T and C-G), read in the opposite direction. For example, with the given sequence ATATCGA, the two types of counting for the 3-mers result in:
*    _non-canonical counting_: ATA 1, TAT 1, ATC 1, TCG 1, CGA 1;
*   _canonical counting_ : ATA 2 (ATA and its reverse complement TAT), ATC 1, CGA 2 (CGA and its reverse complement TCG).

The k-mer counting task is widely used in bioinformatics, and can become a not trivial problem with long genomes, such as Mus musculus (house mouse), and with increasing length of **k**.

The genomic files used in this project are freely available at the following link: https://ftp.ncbi.nlm.nih.gov/genomes/refseq/

## Overview
The project presents three implementation of the k-mer counting algorithm:
*    **parallel version**, which exploits Scala's parallel arrays (`ParArray`) and its parallel collection;
*    **library version**, based on the `NGram` class of Spark's `MLlib` library;
*    **distributed version**,  based on Spark's `RDD` data structure.

All implementations follow the steps:
1. read the genome sequence from file and pre-process it (remove comments and prepare the sequence for the chosen data structure);
2. extract the k-mers and filter the missing information (k-mer containing unknown nucleobases);
3. count k-mers.

# Cloud testing
The project was tested in **Google Cloud Platform** (GCP).
### 1. Project setup
To execute the project in GCP, it is necessary to [enable billing](https://cloud.google.com/billing/docs/how-to/modify-project?hl=en) for the GCP project [previously created](https://cloud.google.com/resource-manager/docs/creating-managing-projects?hl=en). 
Then, enable the GCP services that will be used for the testing on the cloud:
*    Dataproc;
*    Cloud Storage.

### 2. Buckets creation
The Cloud Storage service is used to store all the needed files for the testing, which are: the genome files, the jar file of the project and the output files.

Bucket creation can be done with the following command:
```
gsutil mb -l $REGION gs://$BUCKET_NAME
```
where `$REGION` is the chosen cloud location ([available regions list](https://cloud.google.com/about/locations)), and `$BUCKET_NAME` is the name assigned to the bucket (which has to be a unique string in the whole GCP).

Once the bucket is created, download the genomic files and create the jar for the project (with sbt):
```
sbt clean package
```

Then, copy all the files in the bucket,
```
gsutil $FILE_PATH gs://$BUCKET_NAME/
```
where `$FILE_PATH` is the path to the local file to copy in the bucket.

It is possible to create a second bucket to store the output files of the cloud executions.

### 3. Create a Dataproc cluster
Dataproc clusters can be created with the following command,

```
gcloud dataproc clusters create $CLUSTER_NAME --region $REGION --zone $ZONE \
 --master-machine-type $MASTER_MACHINE_TYPE \
--num-workers $NUM_WORKERS \
--worker-machine-type $WORKER_MACHINE_TYPE
```

where:
*    `$CLUSTER_NAME` is a string specifying the name assigned to the created cluster;
*    `$REGION` and `$ZONE` specify the cluster location;
*    `$MASTER_MACHINE_TYPE` specify, respectively, the master machine type and the workers machine type ([available machine families](https://cloud.google.com/compute/docs/machine-resource));
*    `$NUM_WORKERS` is the number of worker nodes of the cluster.



_**Note**_: this project was developed in according with the cluster image `2.1-debian11` (more details at [2.1.x release versions](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.1?hl=en)). Updates on the default Dataproc cluster image may cause errors in the execution of the project. If so, it is possible to specify the chosen image for the cluster to be created ([Selecting versions](https://cloud.google.com/dataproc/docs/concepts/versioning/overview#selecting_versions)).


### 4. Submitting the job to the cluster
The command to submit a job to the created cluster is:
```
gcloud dataproc jobs submit spark [--id $JOB_ID] [--async] \
--cluster=$CLUSTER_NAME --region=$REGION \
--jar=gs://$BUCKET_NAME/$JAR_FILE_NAME \
-- "yarn" "$BUCKET_NAME/$DATA_NAME" "$K_VALUE" "$COUNTING_TYPE" "$PARTITIONS" "$ALGORITHM" "$OUTPUT_BUCKET"
```
with the following parameters:
*    `$CLUSTER_NAME`, `$REGION` and `$BUCKET_NAME` are defined in the above sections;
*    `$OUTPUT_BUCKET` is the bucket name created to store output files;
*    `$JAR_FILE_NAME` is the name of the jar file (stored in `$BUCKET_NAME`);
*    `$DATA_NAME` is the name of the genomic file (stored in `$BUCKET_NAME`);
*    `$K_VALUE` is the number specifying the length of the k-mer sequence of interest;
*    `$COUNTING_TYPE` can be "canonical" or "non-canonical";
*    `$PARTITIONS` specifies RDD partition number;
*    `$ALGORITHM` can be "parallel", "library" or "distributed".

### 5. Delete cluster
Once all the tests on a specific cluster are executed, delete the cluster:
```
!gcloud dataproc clusters delete $CLUSTER_NAME \
    --region=$REGION
```

### 6. Download results and delete bucket
To download the results of the cloud testing:
```
gsutil cp -r gs://$OUTPUT_BUCKET/
```
Once the output files are downloaded, delete the created bucket (do the same for each bucked creatted):
```
gcloud storage rm --recursive gs://BUCKET_NAME/
```
