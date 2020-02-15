Pour réaliser ce TP, il faut commencer par lancer la commande suivante:
  make prepare-dataset
  
Cela va télécharger les deux datasets et les mettre dans le bon dossier.

Pour la partie développement, je développais à l'intérieur du docker qui va directement nous conduire sur pyspark. En production avec le job spark, j'ai ajouté l'argument -d (detach) qui permet de nous rendre la main pendant que le docker s'exécute.
Dans le Makefile, il suffit de lancer: make run-pyspark
Ensuite il faut relever le n° de container avec la commande: docker ps
Une fois cela fait, il faudra lancer la commande: docker exec -ti <n° de container> bash
Cette dernière commande permet de rentrer dans le container et d'avoir accès à son arborescence.
Finalement pour lancer le job il faut taper:

/spark-2.4.4-bin-hadoop2.7/bin/spark-submit /src/process.py -l data/full.csv -e data/fr-en-adresse-et-geolocalisation-etablissements-premier-et-second-degre.csv -o output/output.csv

*---------------------------*

Pour la clef de jointure, j'ai opté pour le code commune car contrairement au code postal il est unique pour une ville donnée (Paris a autant de codes postals que d'arrondissements, comme Bordeaux, etc.). J'ai du caster cette variable en Integer. J'ai vérifié l'intersection des valeurs distinctes de code communes entre le dataset logements et le dataset ecoles et j'en ai déduis qu'il y a exactement 16322 codes communes de logements qui bénéficient d'au moins une école (sur 21824). Cela m'a conforté pour l'utilisation de la clef de jointure. 
Voila un bout de code très simple qui m'a permis d'y arriver:

lolo = logements.select('code_commune').distinct().collect()
lolo = [x.code_commune for x in lolo]
eco = ecoles.select('ecole_Code_commune').distinct().collect()
eco = [x.ecole_Code_commune for x in eco]

def intersection(lst1, lst2):
        lst3 = []
        for i in lst1:
                if i in lst2:
                        lst3.append(i)
        return lst3

len(lolo)
len(eco)
len(intersection(lolo,eco))


Je me suis aidé des documentations des deux csv pour l'exploration des variables, en partie pour séparer les codes d'établissements correspondant aux premiers et second degré. Aussi pour l'état de l'établissement où les rows avec la valeur "null" ou "à fermer" n'intéressent pas les data scientist.

Le reste du code se trouve dans l'entrypoint process.py.

J'ai rencontré un petit soucis avec la fonction udf (qui permet de décorer une fonction) lorsque j'ai essayé de lui passer deux listes (premier et deuxieme_deg), j'ai donc écris en dur ces listes dans la fonction.
Finalement, la fonction write.parquet() ne tolérant pas les accents j'ai écris la jointure finale dans un csv, après avoir essayé de retirer les accents.
