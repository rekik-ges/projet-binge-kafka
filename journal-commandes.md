# Socle BINGE-1 — Ingestion, validation, DLQ

## Lancer l'application

**1. Démarrer le cluster Kafka local**
```bash
docker compose up -d
```
Lance 3 brokers Kafka + l'interface Kafbat UI (`http://localhost:8080`) pour visualiser les topics.

**2. Lancer l'application**
```powershell
$env:GROUPE = "grp07"
$env:KAFKA_BOOTSTRAP = "localhost:29092"
mvn compile exec:java
```
`GROUPE` préfixe tous nos topics de sortie (ex: `grp07.binge.dlq`) pour ne pas écraser ceux des autres groupes.

---

## Étapes du socle

### 1. Setup projet
Mise en place de l'environnement (JDK 21, Maven, cluster Kafka local via Docker).
**Modification** : aucun code — vérification que le projet compile et que l'appli se connecte au cluster.

### 2. Parsing sûr (poison pill)
Un JSON illisible (tronqué, vide, mal formé) ne doit jamais faire planter l'application en boucle.
**Modification** : on a remplacé la lecture directe du JSON brut par un parsing protégé, `JsonSerdes.parseOrNull()` (fourni), utilisé dans `isValid()` et `rejectReason()` (`BingeTopology.java`) — un JSON illisible devient `null` au lieu de lever une exception.

### 3. Validation des champs requis
Un événement sans `content_id`, `event_type`, etc. ne doit pas être traité comme valide.
**Modification** : on a ajouté la méthode `hasRequiredFields(PlaybackEvent)` (`BingeTopology.java`), qui vérifie que `event_id`, `event_type`, `user_id`, `content_id` et `timestamp` ne sont pas `null`.

### 4. Validation des types et des bornes
Un `position_seconds` négatif ou un `timestamp` illisible faussent les statistiques sans faire planter l'appli.
**Modification** : on a ajouté la méthode `hasValidTypesAndBounds(PlaybackEvent)` (`BingeTopology.java`), qui rejette `position_seconds < 0` et les timestamps qui échouent au parsing `Instant.parse()`.

### 5. Validation des enums
Une valeur comme `"event_type": "PLAYY"` (faute de frappe du SDK) doit être détectée avant de finir en branche morte plus loin.
**Modification** : on a ajouté 3 listes de valeurs autorisées (`VALID_EVENT_TYPES`, `VALID_DEVICES`, `VALID_REGIONS`) et la méthode `hasValidEnums(PlaybackEvent)` (`BingeTopology.java`), qui vérifie que `event_type`/`device`/`region` appartiennent à ces listes.

### 6. Routage valide / invalide
Il faut séparer le flux en deux : les messages qui passent toutes les validations, et les autres.
**Modification** : on a remplacé le flux unique par un `raw.split()...branch()...defaultBranch()` (`BingeTopology.build()`), piloté par la méthode `isValid(String)` qui combine les 3 validations précédentes (tâches 3 à 5) en une seule règle.

### 7. Construction du message DLQ
Un message rejeté sans raison ne sert à rien pour déboguer ou justifier le rejet.
**Modification** : on a ajouté le record `DlqEnvelope(reason, raw)` et la méthode `rejectReason(String)` (`BingeTopology.java`), puis branché la sortie invalide vers `Topics.DLQ` — chaque rejet part avec sa raison précise et le JSON original intact.

### 8. Nettoyage
Le `peek()` fourni servait juste à vérifier la connexion au tout début, il n'a plus d'utilité une fois la validation en place.
**Modification** : on a supprimé les 3 lignes du `peek()` de démonstration dans `BingeTopology.build()`.

### 9. Test de robustesse (10 minutes)
Vérifier que le socle tient face à un vrai volume de données, pas juste sur quelques exemples choisis à la main.
**Modification** : aucun code — rejeu du jeu de données réel (140 418 événements fournis par l'enseignant) vers le cluster local, application laissée active 10 minutes. Résultat : aucun crash, ~6,4% des messages rejetés en DLQ avec raisons motivées, cohérent avec les ~7% d'anomalies annoncées dans le sujet.

**Ce qu'on a fait, avec les commandes :**

1. Rejouer le jeu de données réel (fourni par l'enseignant, dossier `generateurs-v3/data/binge`) vers le cluster local :
```powershell
python rejouer.py --dossier data/binge --project binge --create-topics --replication-factor 1 --bootstrap localhost:29092
```
→ Envoie les 140 418 événements + les 576 fiches catalogue vers `binge.playback.events` et `binge.catalog`.

2. L'application (déjà lancée, cf. section "Lancer l'application") consomme et traite ce flux automatiquement — rien à faire de plus ici.

3. Observer les rejets en direct dans un terminal séparé, pour voir les raisons motivées défiler :
```powershell
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic grp07.binge.dlq --from-beginning
```

4. Laisser l'application tourner 10 minutes sans l'arrêter, en surveillant qu'aucune erreur/exception n'apparaît dans son terminal.

5. Vérifier les volumes dans Kafbat UI (`http://localhost:8080` → Topics) : `binge.playback.events` (140 419 messages) vs `grp07.binge.dlq` (8 925 messages, soit ~6,4%).
