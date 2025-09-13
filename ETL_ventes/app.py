import pandas as pd
from datetime import datetime
import locale

locale.setlocale(locale.LC_ALL, 'fr_FR')

# Chargement des données depus les fichiers CSV sources

# TODO: prendre en charge les autres sources, par exemple produit.csv
# 1. Commencer avec produit.csv dont la classe ProduitInput est précodée
# 2. Continuer avec par exemple Département

ventesInput = pd.read_csv('resources/ventes.csv', delimiter= ';')
cataloguesInput = pd.read_csv('resources/catalogue.csv', delimiter= ',')
clientsInput = pd.read_csv('resources/clients.csv', delimiter= ';')
departementsInput = pd.read_csv('resources/departement.csv', delimiter= ',')
fabricantsInput = pd.read_csv('resources/fabricant.csv', delimiter= ',')
magasinsInput = pd.read_csv('resources/magasin.csv', delimiter= ';')
paysListInput = pd.read_csv('resources/pays.csv', delimiter= ',')
produitsInput = pd.read_csv('resources/produits.csv', delimiter= ';')
villesInput = pd.read_csv('resources/villes_france.csv', delimiter= ',')

# Modèle en étoile
# TODO :
# 1. Ajouter la dimension Produit
# 2. Ajouter les autres dimensions identifiées
    
# Table de faits
ventesFait = []
# Tables de dimensions
clientsDimension = [] 
magasinsDimension = [] 
produitsDimension = [] 
datesDimension = [] 
# result permet de stocker les faits et les dimensions concaténées
result = []

for index,venteInput in ventesInput.iterrows():

    # Récupération des inputs
    # Produit
    produitVente = produitsInput[produitsInput['referenceProduit'] == venteInput['referenceProduit']]
    print(produitVente)
    catalogueProduit = cataloguesInput[cataloguesInput['id'] == produitVente['idCatalogue'].values[0]]
    fabricantProduit = fabricantsInput[fabricantsInput['id'] == catalogueProduit['idFabriquant'].values[0]]
    villeFab = villesInput[villesInput['codeCommune'] == fabricantProduit['Code-commune'].values[0]]
    departementFab = departementsInput[departementsInput['code-insee'] == int(str(villeFab['codePostal'].values[0])[:2])]
    paysFab = paysListInput[paysListInput['alpha-3'] == departementFab['pays-alpha3'].values[0]]
    # Vente
    magasinVente = magasinsInput[magasinsInput['idMagasin'] == venteInput['idMagasin']]
    villeVente = villesInput[villesInput['codeCommune'] == magasinVente['code-commune'].values[0]]
    departementVente = departementsInput[departementsInput['code-insee'] == int(str(villeVente['codePostal'].values[0])[:2])]
    paysVente = paysListInput[paysListInput['alpha-3'] == departementVente['pays-alpha3'].values[0]]
    # Client
    clientInput = clientsInput[clientsInput['id'] == venteInput['idClient']]
    villeClient = villesInput[villesInput['codeCommune'] == clientInput['code-commune'].values[0]]
    departementClient = departementsInput[departementsInput['code-insee'] == int(str(villeClient['codePostal'].values[0])[:2])]
    paysClient = paysListInput[paysListInput['alpha-3'] == departementClient['pays-alpha3'].values[0]]

    # Alimentation des tables de dimensions
    # https://stackoverflow.com/questions/13784192/creating-an-empty-pandas-dataframe-and-then-filling-it
    clientsDimension.append({
        # PK
        'idClient': clientInput['id'].values[0],
        # Hierarchie
        'anneeNaissance': clientInput['anneeNaissance'].values[0],
        'genre': clientInput['genre'].values[0],
        'ville': villeClient['nom'].values[0],
        'latitude': villeClient['latitude'].values[0],
        'longitude': villeClient['longitude'].values[0],
        'codePostal': departementClient['code-nom'].values[0],
        'departement': departementClient['nom'].values[0],
        'pays': paysClient['name'].values[0]
    })
    produitsDimension.append({
        # PK
        'reference':venteInput['referenceProduit'],
        # Hierarchie
        'prixVente':locale.atof((produitVente['montant'].values[0])),
        'nomCatalogue': catalogueProduit['nom'].values[0],
        'nomFab': catalogueProduit['idFabriquant'].values[0],
        'villeFab': villeFab['nom'].values[0],
        'latitudeFab': villeFab['latitude'].values[0],
        'longitudeFab': villeFab['longitude'].values[0],
        'codePostalFab': departementFab['code-nom'].values[0],
        'departementFab': departementFab['nom'].values[0],
        'paysFab': paysFab['name'].values[0]
    })
    magasinsDimension.append({
        # PK
        'idMagasin':venteInput['idMagasin'],
        # Hierarchie
        'ville':villeVente['nom'].values[0],
        'latitude':villeVente['latitude'].values[0],
        'longitude':villeVente['longitude'].values[0],
        'codePostal':departementVente['code-nom'].values[0],
        'departement':departementVente['nom'].values[0],
        'pays':paysVente['name'].values[0]
    })
    t1 = datetime.fromtimestamp(venteInput['date'])
    datesDimension.append({
        # PK
        'timestamp': venteInput['date'],
        # Hierarchie
        'heure':t1.hour,
        'jourSemaine':t1.weekday(),
        'jourAnnee':t1.timetuple().tm_yday,
        'mois':t1.month,
        'annee':t1.year
    })    
    # Alimentation de la table de faits
    ventesFait.append({
        'quantiteProduit':venteInput['quantiteProduit'],
        'montantVendu':locale.atof(venteInput['montant']),
        # FK
        'idClient':venteInput['idClient'],
        'idMagasin':venteInput['idMagasin'],
        'referenceProduit': venteInput['referenceProduit'],
        'dateTimestamp': venteInput['date']
    })
    # Concaténation de tous les résultats
    result.append({
        'quantiteProduit':venteInput['quantiteProduit'],
        'montantVendu':locale.atof(venteInput['montant']),
        # Date
        'timestampVente': venteInput['date'],
        'heureVente':t1.hour,
        'jourSemaineVente':t1.weekday(),
        'jourAnneeVente':t1.timetuple().tm_yday,
        'moisVente':t1.month,
        'anneeVente':t1.year,
        # Magasin
        'idMagasin':venteInput['idMagasin'],
        'villeMagasin':villeVente['nom'].values[0],
        'latitudeMagasin':villeVente['latitude'].values[0],
        'longitudeMagasin':villeVente['longitude'].values[0],
        'codePostalMagasin':departementVente['code-nom'].values[0],
        'departementMagasin':departementVente['nom'].values[0],
        'paysMagasin':paysVente['name'].values[0],
        # Produit
        'reference':venteInput['referenceProduit'],
        'prixVenteProduit':locale.atof(produitVente['montant'].values[0]),
        'nomCatalogueProduit': catalogueProduit['nom'].values[0],
        'nomFab': catalogueProduit['idFabriquant'].values[0],
        'villeFab': villeFab['nom'].values[0],
        'latitudeFab': villeFab['latitude'].values[0],
        'longitudeFab': villeFab['longitude'].values[0],
        'codePostalFab': departementFab['code-nom'].values[0],
        'departementFab': departementFab['nom'].values[0],
        'paysFab': paysFab['name'].values[0],
        # Client
        'idClient': clientInput['id'].values[0],
        'anneeNaissanceClient': clientInput['anneeNaissance'].values[0],
        'genreClient': clientInput['genre'].values[0],
        'villeClient': villeClient['nom'].values[0],
        'latitudeClient': villeClient['latitude'].values[0],
        'longitudeClient': villeClient['longitude'].values[0],
        'codePostalClient': departementClient['code-nom'].values[0],
        'departementClient': departementClient['nom'].values[0],
        'paysClient': paysClient['name'].values[0]
    })

# Conversion des lists en DataFrames
clientsDimension = pd.DataFrame(clientsDimension)
produitsDimension = pd.DataFrame(produitsDimension)
magasinsDimension = pd.DataFrame(magasinsDimension)
datesDimension = pd.DataFrame(datesDimension)
ventesFait = pd.DataFrame(ventesFait)
result = pd.DataFrame(result)

# Écriture des DataFrames dans des fichiers CSV
clientsDimension.to_csv('output/clients-dimension.csv', index=False)
produitsDimension.to_csv('output/produits-dimension.csv', index=False)
magasinsDimension.to_csv('output/magasins-dimension.csv', index=False)
datesDimension.to_csv('output/dates-dimension.csv', index=False)
ventesFait.to_csv('output/ventes-faits.csv', index=False)

# Écriture du fichier csv à importer dans Superset !
result.to_csv('output/result.csv', index=False)
