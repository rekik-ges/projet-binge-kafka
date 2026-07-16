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


