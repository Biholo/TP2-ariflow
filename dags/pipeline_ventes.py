from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def extraire_donnees_ventes():
    donnees = [
        {"produit": "Chaise", "quantite": 12, "prix_unitaire": 45.0},
        {"produit": "Table", "quantite": 3, "prix_unitaire": 120.0},
        {"produit": "Lampe", "quantite": 8, "prix_unitaire": 25.0},
        {"produit": "Chaise", "quantite": 5, "prix_unitaire": 45.0},
    ]
    print(f"Donnees extraites : {len(donnees)} lignes")
    for ligne in donnees:
        print(f"  - {ligne['produit']} x{ligne['quantite']} @ {ligne['prix_unitaire']} euros")
    return donnees


def calculer_totaux(**context):
    ti = context["ti"]
    donnees = ti.xcom_pull(task_ids="extraire_donnees")

    totaux = {}
    for ligne in donnees:
        produit = ligne["produit"]
        montant = ligne["quantite"] * ligne["prix_unitaire"]
        if produit in totaux:
            totaux[produit] += montant
        else:
            totaux[produit] = montant

    total_global = sum(totaux.values())

    print("Totaux par produit :")
    for produit, montant in totaux.items():
        print(f"  {produit} : {montant:.2f} euros")
    print(f"Total global : {total_global:.2f} euros")

    return {"totaux": totaux, "total_global": total_global}


def sauvegarder_rapport(**context):
    ti = context["ti"]
    resultats = ti.xcom_pull(task_ids="calculer_totaux")

    date_rapport = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"Rapport genere le {date_rapport}")
    print(f"Total des ventes : {resultats['total_global']:.2f} euros")
    print("Rapport sauvegarde avec succes (simulation)")


with DAG(
    dag_id="pipeline_ventes",
    description="Pipeline de traitement des ventes journalieres",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ventes", "tp2"],
) as dag:

    tache_extraction = PythonOperator(
        task_id="extraire_donnees",
        python_callable=extraire_donnees_ventes,
    )

    tache_calcul = PythonOperator(
        task_id="calculer_totaux",
        python_callable=calculer_totaux,
    )

    tache_rapport = PythonOperator(
        task_id="sauvegarder_rapport",
        python_callable=sauvegarder_rapport,
    )

    tache_extraction >> tache_calcul >> tache_rapport
