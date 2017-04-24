# Google Cloud Pub/Sub applier.  You must specify a connection string for the server.
replicator.applier.dbms=com.continuent.tungsten.replicator.applier.GCloudPubSubApplier
replicator.applier.dbms.topicUrl=https://pubsub.googleapis.com/v1/{topic}:publish
replicator.applier.dbms.credentialsFile=@{HOME_DIRECTORY}/.gcloud/credentials.json
replicator.applier.dbms.dataSource=global

