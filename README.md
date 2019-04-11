

Principe :

* on ne fait que des imports, mais on veut qu'ils soient "à peu près atomiques" (s'il y a une erreur lors d'un import, on supprime tout ce qui a été importé)
* on a une collection pour tracker les transactions
* les documents importés sont initialement taggés avec l'identifiant de la transaction + statut "non valide" (en cours d'import)
* quand ils sont tous importés:
  - on passe la tx à l'état "commit"
  - on les "committe" en supprimant l'identifiant de la tx + statut "valide"
  - on finit par passer la tx à l'état "done"
* en cas d'erreur lors de la phase d'import:
  - on passe la tx à l'état "rollback"
  - on supprime les documents trackés par la tx
  - on finit par passer la tx à l'état "done"
* en cas d'erreur récupérable (exception...) lors du rollback:
  - on attend un certain temps (backoff) et on réessaie
  - on continue comme ça éternellement avec un backoff exponentiel limité à 30s (ou autre valeur arbitraire)
* en cas d'erreur irrécupérable (crash du process) lors de l'import ou du rollback:
  - un processus tiers (Recovery) identifie les transactions "en cours" depuis trop longtemps (ici 2 minutes pour les tests, mais plus réaliste : 30 minutes, 1 heure...)
  - ces transactions sont rollbackées
* en cas d'erreur irrécupérable lors du commit (transaction à l'état commit):
  - le processus de recovery vient finaliser le commit
* en cas d'erreur (récupérable ou non) dans le recovery, l'invocation suivante du recovery devrait corriger le problème.
  Pour cette raison, il n'y a pas de logique backoff / retry dans le recovery.

Note - le recovery peut fonctionner de manière concurrente avec le rollback. Le seul risque lié au recovery est la
détection d'une transaction "failed" (en état Started depuis plus de n minutes) -> si la tx n'est pas en erreur mais est
toujours en train de se terminer, alors le recovery va rentrer en conflit et commencer à la rollbacker alors que ça ne
devrait pas être le cas. Pour cette raison, si l'objet tx n'est pas en état Started au moment de le passer en Commit,
alors on rentre dans la logique de rollback.
