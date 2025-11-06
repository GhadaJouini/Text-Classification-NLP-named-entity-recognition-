import os
import pandas as pd
import math

def convert_then_split_parquet_safe(file_path: str, max_size_kb: int = 100, sample_lines: int = 500, safety_margin: float = 0.9):
    """
    Convertit un CSV en Parquet et découpe en chunks sous max_size_kb.
    Même si les chunks tests dépassent déjà la limite, ajuste automatiquement.
    """

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} introuvable.")

    parquet_file = file_path.replace(".csv", ".parquet")
    df = pd.read_csv(file_path)
    df.to_parquet(parquet_file, index=False)
    print(f"🔄 Conversion du CSV vers Parquet : {parquet_file}")
    print(f"✅ Fichier Parquet créé : {parquet_file}")

    max_bytes = max_size_kb * 1024
    file_size_bytes = os.path.getsize(parquet_file)
    print(f"📦 Taille du fichier Parquet : {file_size_bytes / 1024:.2f} KB")

    # Si le fichier est déjà inférieur à la limite
    if file_size_bytes <= max_bytes:
        print(f"ℹ️ {parquet_file} est inférieur à {max_size_kb} KB. Pas de découpage nécessaire.")
        return [parquet_file]

    # --- Estimation initiale ---
    sample = df.head(sample_lines)
    temp_sample = "sample_temp.parquet"
    sample.to_parquet(temp_sample, index=False)
    avg_bytes_per_line = os.path.getsize(temp_sample) / len(sample)
    os.remove(temp_sample)

    rows_per_chunk = max(1, int(max_bytes * safety_margin / avg_bytes_per_line))
    print(f"📈 Estimation initiale : {rows_per_chunk} lignes par chunk")

    # --- Fonction pour découper et mesurer ---
    def split_and_measure(rows_per_chunk):
        parts = []
        total_rows = len(df)
        for i, start in enumerate(range(0, total_rows, rows_per_chunk), 1):
            chunk = df.iloc[start:start + rows_per_chunk]
            out = f"data_part_{i:02d}.parquet"
            chunk.to_parquet(out, index=False)
            parts.append((out, os.path.getsize(out)))
        return parts

    # Boucle d'ajustement automatique
    while True:
        parts = split_and_measure(rows_per_chunk)
        max_part_size = max(size for _, size in parts)
        if max_part_size <= max_bytes:
            break
        # Réduction progressive si un chunk dépasse la limite
        reduction_ratio = max_bytes / max_part_size * safety_margin
        new_rows_per_chunk = max(1, int(rows_per_chunk * reduction_ratio))
        if new_rows_per_chunk == rows_per_chunk:
            # On ne peut plus réduire, sortie pour éviter boucle infinie
            print("⚠️ Impossible de respecter la limite exacte, fichier final légèrement supérieur.")
            break
        rows_per_chunk = new_rows_per_chunk
        # Supprimer fichiers générés précédemment
        for f, _ in parts:
            os.remove(f)

    # Affichage final
    for f, size in parts:
        size_kb = size / 1024
        print(f"✅ {f} créé ({size_kb:.2f} KB)")
        if size > max_bytes:
            print(f"⚠️ {f} dépasse la limite de {max_size_kb} KB")

    print(f"🎉 Découpage terminé : {len(parts)} fichiers générés")
    return [f for f, _ in parts]

# Exemple d'utilisation
if __name__ == "__main__":
    convert_then_split_parquet_safe("../donnees/bd_algo_2_version2.csv", max_size_kb=100, sample_lines=1406)
