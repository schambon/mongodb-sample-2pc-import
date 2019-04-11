

Principe :

1  txns.insertOne(new Document("_id", new ObjectId()).append("status", "started").append("lastUpdate", now())

   try {
2     coll.insertMany(allDocumentsToInsert.stream().map(d -> d.append("tx", tx.get("_id"))).collect(toList()))
3     txns.updateOne(eq("_id", tx.get("_id")), combine(set("status", "finishing"), currentDate("lastUpdate"))

   } catch (MongoException e1) {
      logger.error(e1)

      long backoff = 0
      boolean retry = true
      while (retry) {
         try {
            sleep(backoff)
6           txns.updateOne(and(eq("_id", tx.get("_id")), ne("status", "rollback")), combine(set("status", "rollback"), currentDate("lastUpdate"))
7           coll.deleteMany(eq("tx", tx.get("_id")))
8           txns.updateOne(eq("_id", tx.get("_id")), combine(set("status", "done"), currentDate("lastUpdate"))) // or "canceled"?
            retry = false
         } catch (MongoException e2) {
            // log exception but retry anyway
            logger.error(e2)
         } finally {
            backoff = nextBackoff(backoff)
         }
      }
   }
4  coll.updateMany(eq("tx", tx.get("_id")), combine(unset("tx"), set("valid", true)))
5  txns.updateOne(eq("_id", tx.get("_id")), combine(set("status", "done"), currentDate("lastUpdate")))


   long nextBackoff(previous) {
      if (previous == 0) {
         return 100;
      }
      return min(previous * 2, 30000) // double the backoff until we reach 30s, then stay there
   }

Périodiquement (recovery/cleanup thread) :

    // soit on a démarré depuis 30 minutes, soit on est en rollback
    txns.find(or(and(eq("status", "started"), lt("lastUpdate", now().minus(30, MINUTES)), eq("status", "rollback")).forEach( tx -> {
1'      txns.updateOne(and(eq("_id", tx.get("_id")), ne("status", "rollback")), combine(set("status", "rollback"), currentDate("lastUpdate"))
2'      coll.deleteMany(eq("tx", tx.get("_id")))
3'      txns.updateOne(and(eq("_id", tx.get("_id")), ne("status", "done"), combine(set("status", "done"), currentDate("lastUpdate")))
    })

    // resume - si on ne veut pas de ça on peut aussi rajouter la clause "finishing" dans le cleanup au-dessus
    txns.find(eq("status", "finishing")).forEach( tx -> {
4'      coll.updateMany(eq("tx", tx.get("_id")), combine(unset("tx"), set("valid", true)))
5'      txns.updateOne(and(eq("_id", tx.get("_id")), ne("status", "done")), combine(set("status", "done"), currentDate("lastUpdate")))
    })

---


Exception en:

1 -> abort de l'ensemble de la tx, on n'a rien fait

2 -> on passe dans le catch: abort de la tx
3 -> on passe dans le catch: abort de la tx
4 -> on crashe -> ce sera fini par la recovery thread
5 -> on crashe -> ce sera fini par la recovery thread

6 -> backoff and retry
7 -> backoff and retry
8 -> backoff and retry

Crash en:

1 (avant) -> abort de l'ensemble de la tx, on n'a rien fait
1 (après) -> on a créé une tx mais rien rajouté dedans. Elle sera nettoyée par le cleanup thread
2 -> on a créé une tx et des entrées dedans, mais on la laisse "dangling" (non finie) -> sera nettoyée par le cleanup thread
3 -> on a créé une tx et des entrées dedans, mais on la laisse "dangling" (non finie) -> sera nettoyée par le cleanup thread
     (pas moyen de faire autrement, on ne peut pas savoir si on a fini de faire les insertions)
4 -> on a traité une tx mais pas fini de la committer -> sera fini par le recovery/resume thread
5 -> on a traité une tx mais pas fini de la committer -> sera fini par le recovery/resume thread
6 -> la tx et ses docs seront nettoyés par le cleanup thread
7 -> la tx et ses docs seront nettoyés par le cleanup thread
8 -> la tx et ses docs seront nettoyés par le cleanup thread

---

Cleanup/recovery thread

Exception ou crash en:

1' -> sera repris à l'invocation suivante du thread
2' -> la tx est toujours en rollback, le rollback sera terminé à l'invocation suivante du thread
3' -> la tx est toujours en rollback, le rollback sera terminé à l'invocation suivante du thread
4' -> la tx est toujours en finalisation, elle sera terminée à l'invocation suivante du thread
5' -> la tx est toujours en finalisation, elle sera terminée à l'invocation suivante du thread