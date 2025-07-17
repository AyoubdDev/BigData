import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Importation statique des fonctions SQL de Spark.
// C'est une bonne pratique qui rend le code plus concis et lisible.
// Par exemple, au lieu d'écrire "functions.col()", on peut écrire "col()".
import static org.apache.spark.sql.functions.*;

/**
 * Une application Spark qui analyse un fichier CSV d'incidents.
 * Elle calcule le nombre d'incidents par service et identifie les deux années
 * ayant enregistré le plus d'incidents.
 *
 * Cette version utilise l'API programmatique de Spark (DataFrames/Datasets).
 */
public class ServiceIncidents {

    public static void main(String[] args) {

        // 1. Initialisation de la SparkSession
        // C'est le point d'entrée pour toute fonctionnalité de Spark SQL.
        // .master("local[*]") configure Spark pour s'exécuter localement en utilisant tous les cœurs disponibles.
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Analyse des Incidents")
                .getOrCreate();

        // 2. Lecture des données
        // On lit le fichier CSV pour le charger dans un DataFrame (qui est un Dataset<Row>).
        // .option("header", "true") indique que la première ligne du CSV contient les noms des colonnes.
        // .option("inferSchema", "true") demande à Spark de deviner les types de données de chaque colonne (String, Integer, etc.).
        Dataset<Row> incidentsDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("incidents.csv");

        // Il est utile d'afficher le schéma pour vérifier que Spark a bien interprété les types.
        System.out.println("Schéma des données déduit par Spark :");
        incidentsDF.printSchema();
        
        // Affiche un aperçu des données pour s'assurer qu'elles sont chargées correctement.
        System.out.println("Aperçu des 5 premières lignes :");
        incidentsDF.show(5, false);


        // ========================================================================
        // Calcul 1 : Nombre total d'incidents par service
        // ========================================================================

        System.out.println("--- Nombre d'incidents par service ---");

        Dataset<Row> incidentsByService = incidentsDF
                .groupBy(col("service")) // On groupe les lignes par la valeur de la colonne "service".
                .count() // Pour chaque groupe, on compte le nombre de lignes. Spark crée une colonne "count".
                .withColumnRenamed("count", "nb_incidents") // On renomme la colonne "count" pour plus de clarté.
                .orderBy(col("nb_incidents").desc()); // On trie les résultats par ordre décroissant pour voir les services les plus impactés en premier.

        // Affiche le résultat du calcul dans un format tabulaire.
        incidentsByService.show();


        // ========================================================================
        // Calcul 2 : Les deux années avec le plus grand nombre d'incidents
        // ========================================================================

        System.out.println("--- Les 2 années avec le plus d'incidents ---");
        
        Dataset<Row> topTwoYears = incidentsDF
                // On crée une nouvelle colonne "year" en appliquant la fonction "year" sur la colonne "date".
                // La fonction "to_date" convertit d'abord la chaîne de caractères en un type date de Spark.
                .withColumn("year", year(to_date(col("date"))))
                .groupBy(col("year")) // On groupe ensuite par cette nouvelle colonne "year".
                .count() // On compte les incidents pour chaque année.
                .withColumnRenamed("count", "nb_incidents") // On renomme pour la clarté.
                .orderBy(col("nb_incidents").desc()) // On trie pour avoir les années avec le plus d'incidents en premier.
                .limit(2); // On ne garde que les 2 premières lignes du résultat trié.

        // Affiche le résultat final.
        topTwoYears.show();


        // 3. Arrêter la session Spark
        // Libère les ressources utilisées par Spark.
        spark.stop();
    }
}