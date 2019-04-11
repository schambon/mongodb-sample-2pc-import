package org.schambon.test;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static java.lang.Thread.sleep;
import static java.time.Instant.now;
import static org.schambon.test.Util.die;
import static org.schambon.test.Util.exception;

public class Test {

    private static final Logger logger = LoggerFactory.getLogger(Test.class);

    public static final String UPDATE = "lastUpdate";
    public static final String STATUS = "status";
    public static final String TXN_ID = "txnId";
    public static final String MONGODB_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=replset&retryWrites=true";
    public static final String VALID = "valid";


    static int CAPACITY = 10000;

    static SecureRandom random = new SecureRandom();

    public static void main(String[] args) {

        MongoClient client = MongoClients.create(MONGODB_URI);
        MongoDatabase database = client.getDatabase("mydb");
        MongoCollection<Document> collection = database.getCollection("testimport");

        // make sure the collection is empty to start with
        collection.drop();

        // recreate it with a validator to break at a given point...
        database.createCollection("testimport",
                new CreateCollectionOptions().validationOptions(new ValidationOptions().validator(new Document("break", new Document("$exists", false)))));

        // Generate synthetic data to import
        List<Document> documentsToImport = new ArrayList<>(CAPACITY);
        for (int i = 0; i < CAPACITY; i++) {
            documentsToImport.add(generateDocument(i));
        }

        // uncomment to trigger an exception after 5000
        //documentsToImport.get(5000).append("break", true);

        importWithRollback(client, documentsToImport);


    }

    private static void importWithRollback(MongoClient client, List<Document> documentsToImport) {
        MongoDatabase db = client.getDatabase("mydb");
        MongoCollection<Document> txns = db.getCollection("txns");
        MongoCollection<Document> coll = db.getCollection("testimport");

        ObjectId txnId = new ObjectId();
        logger.info("Starting transaction {}", txnId.toString());
        txns.insertOne(new Document("_id", txnId).append(STATUS, Status.STARTED.toString()).append(UPDATE, now()));

        try {
            coll.insertMany(documentsToImport.stream().map(d -> d.append(TXN_ID, txnId).append(VALID, false)).collect(Collectors.toList()));
            // uncomment to test recovery of a dead process while importing
            // die();

            UpdateResult setAsCommittingResult = txns.updateOne(and(eq("_id", txnId), eq(STATUS, Status.STARTED.toString())), combine(set(STATUS, Status.COMMIT.toString()), currentDate(UPDATE)));
            if (setAsCommittingResult.getModifiedCount() != 1) {
                // transaction has been marked as not "Started" (presumably rollback) by another process.
                // to be certain all documents in the tx are deleted, we force it to (re-)rollback
                throw new MongoException("Force rollback"); // not very elegant to throw a MongoException
            }
        } catch (MongoException e1) {
            logger.error("Got an exception while processing transaction - triggering rollback", e1);

            long backoff = 0;
            boolean retry = true;

            while (retry) {
                try {
                    logger.info("Backoff for {}ms", backoff);
                    sleep(backoff);
                    logger.info("Rolling back tx {}", txnId.toString());
                    // mark the transaction as rollback
                    txns.updateOne(and(eq("_id", txnId), ne(STATUS, Status.ROLLBACK.toString())), combine(set(STATUS, Status.ROLLBACK.toString()), currentDate(UPDATE)));
                    coll.deleteMany(eq(TXN_ID, txnId));

//                    // test error in middle of rollback...
//                    coll.deleteMany(and(eq(TXN_ID, txnId), lt("_id", 500)));
//                    if (backoff == 0) exception();
//                    coll.deleteMany(and(eq(TXN_ID, txnId), lt("_id", 1000))); // resume deletion

//                    // test failover
//                    if (backoff == 100) client.getDatabase("admin").runCommand(new Document("replSetStepDown", 60));
//
//                    coll.deleteMany(and(eq(TXN_ID, txnId), lt("_id", 2000))); // resume deletion
//
//                    // test process crash
//                    if (backoff >= 200) die();

//                    // the rest is academic because it never reaches here
//                    coll.deleteMany(eq(TXN_ID, txnId));

                    txns.updateOne(eq("_id", txnId), combine(set(STATUS, Status.DONE.toString()), currentDate(UPDATE)));
                    retry = false; // exit the backoff/retry loop

                    logger.info("Rolled back transaction {}", txnId.toString());
                } catch (InterruptedException ignored) {
                    logger.info("Interrupted");
                } catch (RuntimeException e2) {
                    logger.error("Exception while rolling back a transaction - backing off and retrying", e2);
                } finally {
                    backoff = nextBackoff(backoff);
                }
            }
        }

        // test error while committing
//        exception();

        coll.updateMany(eq(TXN_ID, txnId), combine(unset(TXN_ID), set(VALID, true)));
        UpdateResult result = txns.updateOne(and(eq("_id", txnId), ne(STATUS, Status.DONE.toString())), combine(set(STATUS, Status.DONE.toString()), currentDate(UPDATE)));
        if (result.getModifiedCount() == 1) logger.info("Committed transaction {}", txnId.toString());

    }

    private static long nextBackoff(long backoff) {
        return Math.max(100, Math.min(backoff * 2, 30000));
    }


    static Document generateDocument(int i) {
        return new Document("_id", i).append("value", random.nextInt());
    }
}
