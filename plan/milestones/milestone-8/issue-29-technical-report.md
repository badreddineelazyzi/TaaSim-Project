## Rapport Final du Projet TaaSim : Transport as a Service

**Équipe :** Abdelkabir ELGUARTOUFI, Tamzirt, El azzyzi, Wissal, Said  
**Date :** Juin 2026  
**Livrables joints :** Code source (ZIP/Lien GitHub), Extraits de données (CSV/JSON)

---

## 1. Méthodologies

### 1.1 Problématique et Solution
Le transport urbain moderne nécessite une compréhension en temps réel de la demande et de l'offre pour optimiser les trajets. **TaaSim** répond à ce besoin en ingérant des milliers d'événements GPS par seconde, en les normalisant, et en prédisant la demande future par zone géographique à Casablanca.

### 1.2 Choix d'Architecture (Architecture Kappa)
Nous avons opté pour une **Architecture Kappa** afin de traiter les données historiques et en temps réel via un pipeline unique. Cela réduit la complexité de maintenance par rapport à une architecture Lambda et garantit une cohérence parfaite entre l'entraînement des modèles et l'inférence en direct.

### 1.3 Stack Technologique
* **Ingestion :** Apache Kafka (pour le buffering haute fréquence).
* **Traitement (Stream/Batch) :** Apache Flink (Stateful processing, Watermarks).
* **Stockage :** Apache Cassandra (optimisé pour les lectures temporelles) et MinIO (Object storage pour les checkpoints).
* **Machine Learning :** Spark MLlib / Scikit-Learn avec FastAPI pour le serving.
* **Visualisation :** Grafana connecté directement à Cassandra.

---

## 2. Réalisations

### 2.1 Ingestion et Traitement des Données en Temps Réel
Nous avons implémenté avec succès un pipeline Flink robuste composé de plusieurs jobs :
* **Job 1 (GPS Normalizer) :** Nettoyage des données Kafka `raw.gps`, filtrage des anomalies de vitesse (> 150 km/h), gestion des événements en retard via des Watermarks de 3 minutes, et anonymisation des coordonnées vers les centroïdes des zones de Casablanca.
* **Job 2 (Demand Aggregator) & Job 3 (Trip Matcher) :** Agrégation en temps réel pour calculer le ratio offre/demande par zone.

### 2.2 Pipeline de Machine Learning
Pour anticiper les besoins, nous avons développé un modèle prédictif :
* **Feature Engineering :** Extraction d'indicateurs temporels et spatiaux à partir de notre base de données.
* **Modèle :** Déploiement d'un modèle (ex: Gradient Boosted Trees) pour prévoir la demande à court terme. Notre modèle surpasse la baseline historique (RMSE) sur les zones critiques de Casablanca.

### 2.3 Visualisation et Monitoring
Mise en place d'un dashboard interactif sous **Grafana**. Ce tableau de bord consomme les données normalisées depuis Cassandra pour afficher :
* Une carte géographique (Geomap) des ratios de demande par zone.
* L'état du réseau en direct, facilitant les opérations et le dispatching des véhicules.

---

## 3. Conclusions

### 3.1 Bilan des Performances
Le système TaaSim a atteint ses objectifs de traitement en temps réel. Le pipeline Flink a démontré sa capacité à gérer de larges volumes de pings GPS, tout en maintenant l'intégrité de l'état (Stateful deduplication) et en gérant efficacement les checkpoints vers MinIO toutes les 60 secondes.

### 3.2 Leçons Apprises
* **Ce qui a bien fonctionné :** La séparation des responsabilités entre Kafka (Message Broker) et Flink (Stream Processing) a rendu le système très résilient.
* **Défis rencontrés :** La gestion des `Watermarks` pour le traitement des événements tardifs s'est révélée complexe, tout comme la configuration du broadcast state pour le mapping des zones.
* **Perspectives :** Si le projet devait se poursuivre, nous investirions davantage dans l'optimisation des requêtes Cassandra (partition keys) pour accélérer le chargement des dashboards complexes dans Grafana.