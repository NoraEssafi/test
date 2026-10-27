import os
python_path = r"C:\Users\lenovo\AppData\Local\Programs\Python\Python313\python.exe"

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# Configuration Hadoop Windows
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, window 
from pyspark.sql.window import Window
from pyspark.sql.functions import count , col , row_number , avg , max , min , when , sum
spark = (
    SparkSession.builder
    .appName("AnalyseVentes")
    .config("spark.hadoop.home.dir", r"C:\hadoop")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Lire les fichiers Parquet
df = spark.read.parquet(
    r"C:\Users\lenovo\Documents\projet_kafka_pyspark\data\ventes"
)

print("=== DONNÉES DES VENTES ===")

#df.show()
"""
print("=== STRUCTURE ===")

df.printSchema()
print("=== CHIFFRE D'AFFAIRES PAR PRODUIT ===")

ca_produit = (
    df.groupBy("produit")
      .sum("montant")
      .withColumnRenamed("sum(montant)", "chiffre_affaires")
      .orderBy("chiffre_affaires", ascending=False)
)

ca_produit.show()

print("=== CHIFFRE D'AFFAIRES TOTAL ===")

ca_total = (
    df.agg(
        spark_sum("montant").alias("chiffre_affaires_total")
    )
)

ca_total.show()

print("=======Quantite total des produit =====")

quantite_produit=(
    df.groupBy("produit").agg(
        spark_sum("quantite").alias("quantite_totale")
      )
      .orderBy("quantite_totale", ascending=False)
)

quantite_produit.show()


print ("=====Nombre de vente par produit =====")

nb_ventes = (df.groupBy("produit").agg(
    count("*").alias("nombre_ventes"))
    .orderBy("nombre_ventes", ascending=False)
)
nb_ventes.show()


print("===informations generales =====")
info_produit = (df.groupBy("produit").agg(
    count("*").alias("nombre_ventes"),
    spark_sum("quantite").alias("quantite_total"),
    spark_sum("montant").alias("chiffre_affaires"))
)
info_produit.show()

print("=== maximum par produit =====")
maximum = (df.groupBy("produit").agg(
    max("quantite").alias("quantite_maximum"),
   max("montant").alias("montant_max"))
)
maximum.show()

minimum = (df.groupBy("produit").agg(
    min("quantite").alias("quantite_min"),
   min("montant").alias("montant_min"))
)
minimum.show()

print("=== VENTES SUPÉRIEURES À 10 000 DH et sup a 3 articles ===")
ventes_importantes = df.filter((col("montant")>10000) & (col("quantite")>3))

ventes_importantes.show()


print("=== seletion  =====")
selection = df.select (
    "produit",
    "quantite",
    "montant"
)
selection.show()


print("select avec filter")
slct_filtrage = (df.filter(col("montant")>10000)
    .select(
        "produit",
        "quantite",
        "montant"))

slct_filtrage.show()

print ("select filter and or")
resultat = (df.filter((col("montant")>20000) &
           ( (col("produit") == "Laptop") |
            (col("produit") == "PC Bureau")))
            .select(
                "produit",
                "quantite",
                "montant"
            )
)
resultat.show()       



print("test laptop")
test = df.filter(col("produit")=="Laptop").select(
    "produit",
    "quantite",
    "montant"
)
test.show()

print("creation de la nouvelle colonne: categorie ")
categorie = df.withColumn("niveau_vente", 
    when(col("montant")>=30000  , "Grande")
    .when(col("montant")>10000 , "Moyenne")
    .otherwise("Petite")
    )
categorie.show()


print("creation de la nouvelle colonne : niveau_quantite")
niveau_quantite = df.withColumn("niveau_quantite",
        when(col("quantite")>=5 , "Grande")
        .when(col("quantite") >=3 , "Moyenne")
        .otherwise("Petite")  )
niveau_quantite.show()

print("=== NOMBRE DE VENTES PAR NIVEAU ===")

resultat = (
    niveau_quantite
    .groupBy("niveau_quantite")
    .agg(
        count("*").alias("nombre_ventes")
    )
    .orderBy("nombre_ventes", ascending=False)
)

resultat.show()

print("quantite totale par niveau : ")
resultat = (niveau_quantite.groupBy("niveau_quantite").agg(
    sum ("quantite").alias("quantite_totale")
)
.orderBy("quantite_totale", ascending= False))
resultat.show()

print("=== QUANTITÉ MOYENNE PAR NIVEAU ===")
resultat=(niveau_quantite.groupBy("niveau_quantite").agg(
    count("*").alias("nbre_vente"),
    avg("quantite").alias("quantite_moyenne"),
    max("quantite").alias("quantite_maximale"),
    spark_sum("quantite").alias("quantite_totale")  )
    .orderBy("niveau_quantite")
)
resultat.show()


print("produit distincts : ")
produits = (df.select("produit").distinct())
produits.show()


print("drop les duplications : ")
sans_duplication = df.dropDuplicates(["produit"])
sans_duplication.show()


print("=== PRODUITS ===")

produits = spark.createDataFrame([
    ("Souris", "Informatique"),
    ("Clavier", "Informatique"),
    ("Laptop", "Informatique"),
    ("PC Bureau", "Informatique"),
    ("Ecran", "Informatique"),
    ("Casque", "Audio"),
    ("TV", "Multimédia")
], ["produit", "categorie"])

produits.show()

print("=== VENTES AVEC CATÉGORIE ===")
df_join = df.join(produits, 
                  on= "produit",
                  how= "inner")
df_join.show()


print("=== CHIFFRE D'AFFAIRES PAR CATÉGORIE ===")

resultat = (df_join
    .groupBy("categorie")
    .agg(
        spark_sum("montant").alias("chiffre_affaires")
    )
    .orderBy("chiffre_affaires", ascending=False)
)

resultat.show()


print("=== PRODUITS ===")

produits = spark.createDataFrame([
    ("Souris", "Informatique"),
    ("Clavier", "Informatique"),
    ("Laptop", "Informatique"),
    ("PC Bureau", "Informatique"),
    ("Ecran", "Informatique"),
    ("Casque", "Audio")
], ["produit", "categorie"])

produits.show()

print("=== VENTES AVEC CATÉGORIE ===")
df_join = df.join(produits, 
                  on= "produit",
                  how= "left")
df_join.show()


print("=== CHIFFRE D'AFFAIRES PAR CATÉGORIE ===")

resultat = (df_join
    .groupBy("categorie")
    .agg(
        spark_sum("montant").alias("chiffre_affaires")
    )
    .orderBy("chiffre_affaires", ascending=False)
)

resultat.show()


print("=== PRODUITS SANS CATÉGORIE ===")

resultat = df_join.filter(
    col("categorie").isNull()).agg(
        count("*").alias("nombre_ventes"),
        spark_sum("montant").alias("chiffre_affaires")

    )
resultat.show()





produits = spark.createDataFrame([
    ("Souris", "Informatique"),
    ("Clavier", "Informatique"),
    ("Laptop", "Informatique"),
    ("PC Bureau", "Informatique"),
    ("Ecran", "Informatique"),
    ("Casque", "Audio"),
    ("TV", "Multimédia"),
    ("Imprimante", "Informatique")
], ["produit", "categorie"])

print("=== VENTES AVEC CATÉGORIE ===")
df_join = df.join(produits, 
                  on= "produit",
                  how= "right")
df_join.show()


print("=== CHIFFRE D'AFFAIRES PAR CATÉGORIE ===")

resultat = (df_join
    .groupBy("categorie")
    .agg(
        spark_sum("montant").alias("chiffre_affaires")
    )
    .orderBy("chiffre_affaires", ascending=True)
)

resultat.show()


print("=== PRODUITS SANS VENTE ===")

resultat = (
    df_join
    .filter(col("id").isNull())
    .select("produit", "categorie")
)
resultat.show()


print("=== FULL OUTER JOIN ===")
produits = spark.createDataFrame([
    ("Souris", "Informatique"),
    ("Clavier", "Informatique"),
    ("Laptop", "Informatique"),
    ("PC Bureau", "Informatique"),
    ("Ecran", "Informatique"),
    ("Casque", "Audio"),
    ("TV", "Multimédia")
], ["produit", "categorie"])



df_full = df.join(
    produits,
    on="produit",
    how="full"
)

#df_full.show()


resultat = (
    df_full
    .filter(
        col("id").isNull() | col("categorie").isNull()
    )
    .select("produit", "categorie")
)

resultat.show()

# gestion des valeurs manquantes
df_corrige = df_full.fillna({
    "prix": 0,
    "quantite": 0,
    "montant": 0
})
df_corrige.filter(
    col("produit") == "Imprimante"
).show()

print("=== SUPPRESSION DES VALEURS MANQUANTES ===")
df_sans_null = df_full.dropna(how="any")

df_sans_null.show()
df_sans_null.count()


print("AVANT COUNT")

#resultat = df_full.dropna(how="any").count()

resultat = df_full.dropna(how="all").count()


print("NOMBRE DE LIGNES :", resultat)



df_sans_prix_null= df_full.dropna(subset = ["prix"])
print("NOMBRE DE LIGNES sans prix null :", df_sans_prix_null.count())


df_sans_id_null= df_full.dropna(subset = ["id"])
print("NOMBRE DE LIGNES sans id null :", df_sans_id_null.count())



fenetre = Window.partitionBy("id").orderBy(col("prix").desc())
df_classe = df.withColumn("rang", row_number().over(fenetre))
df_classe.show()
df_unique =df_classe.filter(col("rang") == 1).drop("rang")
df_unique.show()
"""

fenetre = Window.partitionBy("produit")

df_analyse =df.withColumn("ca_total_produit", sum("montant").over(fenetre))
df_analyse = df_analyse.withColumn("prix_moyen_produit", avg("prix").over(fenetre))
df_analyse.select("produit", "quantite", "montant", "ca_total_produit", "prix_moyen_produit").show()                         


fenetre_cumul = (
    Window
    .partitionBy("produit")
    .orderBy("id")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

df_cumul = df.withColumn(
    "ca_cumule",
    sum("montant").over(fenetre_cumul)
)

df_cumul.select(
    "id", "produit", "montant", "ca_cumule"
).show()