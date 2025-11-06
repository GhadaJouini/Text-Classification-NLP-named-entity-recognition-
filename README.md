import os

def delete_parquet_files(folder_path: str):
    """
    Supprime tous les fichiers .parquet dans le dossier spécifié.
    
    Args:
        folder_path (str): chemin du dossier à nettoyer
    """
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Le dossier {folder_path} n'existe pas.")

    # Lister tous les fichiers dans le dossier
    files = os.listdir(folder_path)
    deleted_files = []

    for file in files:
        file_path = os.path.join(folder_path, file)
        # Vérifier si c'est un fichier parquet
        if os.path.isfile(file_path) and file.lower().endswith(".parquet"):
            os.remove(file_path)
            deleted_files.append(file_path)
            print(f"✅ Supprimé : {file_path}")

    if not deleted_files:
        print("ℹ️ Aucun fichier .parquet trouvé dans le dossier.")
    else:
        print(f"🎉 Total fichiers supprimés : {len(deleted_files)}")

    return deleted_files


# Exemple d'utilisation
if __name__ == "__main__":
    folder_to_clean = "../donnees"
    delete_parquet_files(folder_to_clean)
