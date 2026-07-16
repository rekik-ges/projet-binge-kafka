# Journal des commandes — Projet BINGE

Fichier personnel de suivi (pas destiné au rendu final).
Chaque section = une petite tâche, avec les commandes exécutées.

---

## Ticket socle (BINGE-1)

### Étape 1 — setup projet

```bash
# Créer .gitignore à la racine avec ce contenu :
target/
*.class
.idea/
*.iml
.vscode/
.DS_Store

# Vérifier que le projet compile
mvn compile

# Démarrer l'environnement Kafka local (3 brokers + Kafbat UI)
docker compose up -d

# Vérifier que les conteneurs tournent
docker compose ps

# Confirmer dans le navigateur
http://localhost:8080

# Ajouter les fichiers au commit
git add .

# Vérifier ce qui est prêt à être commité
git status

# Commit
git commit -m "socle - mise en place du projet"
```

Prérequis installés en cours de route (absents au départ) :
```bash
# JDK 21 (le projet l'exige, seul le JDK 11 était installé)
winget install EclipseAdoptium.Temurin.21.JDK

# Maven (absent, pas trouvé via winget -> installation manuelle)
# téléchargé depuis https://maven.apache.org/download.cgi (apache-maven-3.9.16-bin.zip)
# extrait dans C:\maven

# Ajouter Maven au PATH utilisateur
[Environment]::SetEnvironmentVariable("Path", $env:Path + ";C:\maven\apache-maven-3.9.16\bin", "User")

# Pointer JAVA_HOME vers le JDK 21 (sinon Maven prend le JDK 11 par défaut)
[Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot", "User")
```

Après un redémarrage du PC, si `docker ps` renvoie une erreur "Internal Server Error" :
```bash
docker context use desktop-linux   # si le contexte CLI pointe sur le mauvais pipe
wsl --shutdown                     # relance propre du moteur Docker (WSL2)
# puis : Quitter Docker Desktop et le rouvrir, attendre "Engine running"
```

Piège rencontré : `.gitignore` s'est retrouvé encodé en UTF-16 (via l'IDE), ce qui empêchait
Git de lire les règles correctement (`target/` n'était pas ignoré). Réécrit en UTF-8 via :
```bash
printf 'target/\n*.class\n.idea/\n*.iml\n.vscode/\n.DS_Store\n' > .gitignore
```

---

### Étape 2 — parsing sûr (poison pill)

Modification dans `BingeTopology.java` : ajout des imports `PlaybackEvent` et `JsonSerdes`,
puis ajout de la transformation :
```java
// Parsing sûr (poison pill)
KStream<String, PlaybackEvent> parsed = raw.mapValues(
        value -> JsonSerdes.parseOrNull(value, PlaybackEvent.class));
```

```bash
# Vérifier que ça compile (forcé, pour éviter un faux "Nothing to compile")
mvn clean compile

git add .gitignore src/main/java/fr/esgi/kafka/binge/BingeTopology.java
git commit -m "socle - parsing JSON sans crash sur poison pill"
```

Correction annexe : `target/` était suivi par Git depuis le tout premier commit (avant que
le `.gitignore` soit actif). Retiré du suivi sans supprimer les fichiers du disque :
```bash
git rm -r --cached target/
git commit -m "socle - retrait de target/ du suivi git (fichiers compiles)"
```

---

### Étape 3 — validation des champs requis (à venir)
