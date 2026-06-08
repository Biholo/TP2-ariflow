# TP2 - Premier DAG Airflow

## Lancer l'environnement

```bash
docker compose up -d --wait
```

L'interface web est accessible sur `http://localhost:8080`  
Login : admin / admin

## Le DAG

Le fichier `dags/pipeline_ventes.py` contient un pipeline de traitement de ventes journalieres avec 3 taches :

**extraire_donnees** : simule la lecture d'une source de donnees (ici une liste codee en dur). Retourne les lignes brutes.

**calculer_totaux** : recupere les donnees via XCom et calcule les totaux par produit ainsi que le chiffre d'affaires global.

**sauvegarder_rapport** : recupere les resultats et genere le rapport final (simulation par print, ce serait une ecriture en base ou fichier en production).

Les dependances sont definies explicitement en bas du fichier :

```python
tache_extraction >> tache_calcul >> tache_rapport
```

## Lancer le DAG manuellement

Via l'interface web : cliquer sur le DAG `pipeline_ventes`, puis sur le bouton "Trigger DAG".

Via le terminal :

```bash
docker compose exec airflow-webserver airflow dags trigger pipeline_ventes
```

## Consulter les logs

Les logs sont disponibles dans le dossier `logs/` ou directement depuis l'interface web en cliquant sur une tache dans la vue Graph.

## Comment ca fonctionne

Airflow orchestre des taches sous forme de graphe oriente (DAG). Chaque tache est independante et Airflow s'occupe de les executer dans le bon ordre en respectant les dependances definies.

Le scheduler surveille en permanence les DAGs et planifie les taches a executer. Le webserver expose l'interface de suivi. Les deux partagent la meme base de donnees Postgres qui stocke l'etat de chaque execution.

Dans ce pipeline, les taches communiquent entre elles via XCom : `extraire_donnees` pousse ses resultats, `calculer_totaux` les recupere pour les traiter, puis `sauvegarder_rapport` recupere les totaux calcules. Sans XCom, chaque tache serait totalement isolee et ne pourrait pas utiliser ce qu'a produit la precedente.

Le fait de decouper en 3 taches separees plutot qu'une seule fonction permet de voir dans les logs exactement ou ca echoue si quelque chose plante, et de relancer uniquement la tache en erreur sans tout recommencer depuis le debut.

## Arreter l'environnement

```bash
docker compose down
```
