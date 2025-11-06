import os
import math
import pandas as pd

def convert_then_split_parquet(file_path: str, max_size_kb: int = 400, sample_lines: int = 500):
    """
    Convertit un fichier CSV en un fichier Parquet complet, puis le divise en plusieurs
    fichiers Parquet selon une taille maximale donnée (max_size_kb).
    """

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} introuvable.")

    # --- Étape 1 : Conversion du CSV vers un fichier Parquet complet ---
    parquet_file = file_path.replace(".csv", ".parquet")
    print(f"🔄 Conversion du CSV vers Parquet : {parquet_file}")

    df = pd.read_csv(file_path)
    df.to_parquet(parquet_file, index=False)
    print(f"✅ Fichier Parquet créé : {parquet_file}")

    # --- Étape 2 : Calcul de la taille et préparation du découpage ---
    max_size_bytes = max_size_kb * 1024             # 👉 conversion KB → octets
    file_size_bytes = os.path.getsize(parquet_file)
    print(f"📦 Taille du fichier Parquet : {file_size_bytes / 1024:.2f} KB")

    # Échantillonnage pour estimer la taille moyenne d'une ligne
    sample = df.head(sample_lines)
    temp_sample_path = "sample_temp.parquet"
    sample.to_parquet(temp_sample_path, index=False)
    sample_size_bytes = os.path.getsize(temp_sample_path)
    avg_line_bytes = sample_size_bytes / len(sample)
    os.remove(temp_sample_path)

    expected_parts = math.ceil(file_size_bytes / max_size_bytes)
    rows_per_chunk = max(1, int((max_size_bytes * 0.9) / avg_line_bytes))

    print(f"🧩 Fichier estimé à {expected_parts} parties")
    print(f"📈 Lignes estimées par partie : {rows_per_chunk}")

    # --- Étape 3 : Découpage du Parquet ---
    part_files = []
    total_rows = len(df)
    for i, start in enumerate(range(0, total_rows, rows_per_chunk), start=1):
        chunk = df.iloc[start:start + rows_per_chunk]
        out = f"data_part_{i:02d}.parquet"
        chunk.to_parquet(out, index=False)
        part_files.append(out)

        size_kb = os.path.getsize(out) / 1024
        print(f"✅ {out} créé — {size_kb:.2f} KB")

    print(f"🎉 Découpage terminé : {len(part_files)} fichiers générés")

    # --- Étape 4 : Vérification des tailles ---
    for p in part_files:
        s = os.path.getsize(p)
        if s > max_size_bytes:
            print(f"⚠️ {p} = {s/1024:.2f} KB (dépasse la limite de {max_size_kb} KB)")

    return part_files


# === Exemple d'utilisation ===
if __name__ == "__main__":
    parts = convert_then_split_parquet("data.csv", max_size_kb=400, sample_lines=300)
