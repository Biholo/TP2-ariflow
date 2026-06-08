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

## Arreter l'environnement

```bash
docker compose down
```
