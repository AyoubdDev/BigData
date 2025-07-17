import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

public class Main {

    // ========================================================================
    // Classe de données interne (POJO) pour représenter une ligne de vente.
    // Implémente Serializable pour être utilisée par Spark à travers le cluster.
    // ========================================================================
    public static class Vente implements Serializable {
        private String date;
        private String ville;
        private String produit;
        private int prix;

        // Constructeur, getters et setters
        public Vente() {}

        public Vente(String date, String ville, String produit, int prix) {
            this.date = date;
            this.ville = ville;
            this.produit = produit;
            this.prix = prix;
        }

        public String getDate() { return date; }
        public void setDate(String date) { this.date = date; }
        public String getVille() { return ville; }
        public void setVille(String ville) { this.ville = ville; }
        public String getProduit() { return produit; }
        public void setProduit(String produit) { this.produit = produit; }
        public int getPrix() { return prix; }
        public void setPrix(int prix) { this.prix = prix; }

        // Méthode statique pour parser une ligne de texte et créer un objet Vente
        public static Vente fromString(String line) {
            try {
                String[] parts = line.split(" ");
                return new Vente(parts[0], parts[1], parts[2], Integer.parseInt(parts[3]));
            } catch (Exception e) {
                System.err.println("Ligne mal formée, ignorée : " + line);
                return null; // Retourne null pour les lignes invalides
            }
        }
    }

    // ========================================================================
    // Méthode principale de l'application Spark
    // ========================================================================
    public static void main(String[] args) {
        // Initialisation de la session Spark
        SparkSession spark = SparkSession.builder()
                .appName("Ventes avec Datasets")
                .master("local[*]")
                .getOrCreate();

        // Lecture du fichier texte en un Dataset de chaînes de caractères
        Dataset<String> rddLines = spark.read().textFile("/app/ventes.txt");

        // Compter le nombre total de ventes
        long totalVentes = rddLines.count();
        System.out.println("Total Ventes : " + totalVentes);

        // Conversion du Dataset de chaînes en un Dataset typé de Vente
        Encoder<Vente> venteEncoder = Encoders.bean(Vente.class);
        Dataset<Vente> dsVentes = rddLines.map(Vente::fromString, venteEncoder)
                                          .filter(vente -> vente != null); // Filtrer les lignes mal formées

        // ----- Calcul 1 : Total des ventes par ville -----
        System.out.println("\n--- Total des ventes par ville ---");
        Dataset<Row> totalParVille = dsVentes
                .groupBy("ville") // Grouper par la colonne "ville"
                .agg(sum("prix").as("total_prix")) // Agréger en sommant la colonne "prix"
                .orderBy(col("ville").asc()); // Trier par ville pour un affichage ordonné

        totalParVille.show(); // Affiche le résultat dans un format de tableau

        // ----- Calcul 2 : Total des ventes par année et par ville -----
        System.out.println("\n--- Total des ventes par année et par ville ---");

        // Ajout d'une colonne "annee" extraite de la colonne "date"
        Dataset<Row> totalParAnneeVille = dsVentes
                .withColumn("annee", year(to_date(col("date"), "dd/MM/yyyy")))
                .groupBy("ville", "annee") // Grouper par ville ET année
                .agg(sum("prix").as("total_prix")) // Agréger les prix
                .orderBy("ville", "annee"); // Trier pour un affichage clair

        totalParAnneeVille.show(); // Affiche le résultat

        // On peut aussi utiliser directement Spark SQL pour un style déclaratif
        System.out.println("\n--- Même calcul avec une requête Spark SQL ---");
        // Crée une "vue" temporaire sur laquelle on peut exécuter des requêtes SQL
        dsVentes.createOrReplaceTempView("ventes_view");

        Dataset<Row> resultatSql = spark.sql(
            "SELECT ville, YEAR(TO_DATE(date, 'dd/MM/yyyy')) as annee, SUM(prix) as total_prix " +
            "FROM ventes_view " +
            "GROUP BY ville, annee " +
            "ORDER BY ville, annee"
        );

        resultatSql.show();

        // Arrêter la session Spark
        spark.stop();
    }
}