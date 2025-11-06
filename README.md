import os
import math
import pandas as pd

def convert_then_split_parquet_dynamic(file_path: str, max_size_kb: int = 400, sample_lines: int = 500):
    """
    Convertit un fichier CSV en Parquet, puis le découpe en plusieurs fichiers Parquet.
    Le découpage est ajusté dynamiquement à partir des tailles réelles des deux premiers morceaux.
    """

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} introuvable.")

    # --- Étape 1 : Conversion du CSV vers Parquet ---
    parquet_file = file_path.replace(".csv", ".parquet")
    print(f"🔄 Conversion du CSV vers Parquet : {parquet_file}")

    df = pd.read_csv(file_path)
    df.to_parquet(parquet_file, index=False)
    print(f"✅ Fichier Parquet créé : {parquet_file}")

    # --- Étape 2 : Estimation initiale ---
    max_size_bytes = max_size_kb * 1024
    file_size_bytes = os.path.getsize(parquet_file)
    print(f"📦 Taille du fichier Parquet : {file_size_bytes / 1024:.2f} KB")

    sample = df.head(sample_lines)
    temp_sample_path = "sample_temp.parquet"
    sample.to_parquet(temp_sample_path, index=False)
    sample_size_bytes = os.path.getsize(temp_sample_path)
    avg_line_bytes = sample_size_bytes / len(sample)
    os.remove(temp_sample_path)

    # Estimation initiale du nombre de lignes par partie
    rows_per_chunk = max(1, int((max_size_bytes * 0.8) / avg_line_bytes))
    print(f"📈 Estimation initiale : {rows_per_chunk} lignes par chunk")

    # --- Étape 3 : Création des deux premiers chunks test ---
    test_parts = []
    for i, start in enumerate(range(0, rows_per_chunk * 2, rows_per_chunk), start=1):
        if start >= len(df):
            break
        chunk = df.iloc[start:start + rows_per_chunk]
        out = f"data_part_test_{i:02d}.parquet"
        chunk.to_parquet(out, index=False)
        size_kb = os.path.getsize(out) / 1024
        test_parts.append(size_kb)
        print(f"🧩 Test {i}: {out} créé ({size_kb:.2f} KB)")

    # --- Étape 4 : Ajustement dynamique ---
    if len(test_parts) >= 2:
        avg_test_size_kb = sum(test_parts) / len(test_parts)
        print(f"📊 Taille moyenne réelle des 2 premiers fichiers : {avg_test_size_kb:.2f} KB")
        adjustment_ratio = max_size_kb / avg_test_size_kb
        rows_per_chunk = int(rows_per_chunk * adjustment_ratio * 0.9)  # marge de sécurité
        print(f"🔧 Ajustement : {rows_per_chunk} lignes par chunk après calibration")

    # Supprimer les fichiers tests avant le vrai découpage
    for t in os.listdir():
        if t.startswith("data_part_test_"):
            os.remove(t)

    # --- Étape 5 : Découpage final avec la taille ajustée ---
    part_files = []
    total_rows = len(df)
    for i, start in enumerate(range(0, total_rows, rows_per_chunk), start=1):
        chunk = df.iloc[start:start + rows_per_chunk]
        out = f"data_part_{i:02d}.parquet"
        chunk.to_parquet(out, index=False)
        part_files.append(out)
        size_kb = os.path.getsize(out) / 1024
        print(f"✅ {out} créé ({size_kb:.2f} KB)")

    print(f"🎉 Découpage terminé : {len(part_files)} fichiers générés")

    # --- Étape 6 : Vérification finale ---
    for p in part_files:
        s = os.path.getsize(p)
        if s > max_size_bytes:
            print(f"⚠️ {p} = {s/1024:.2f} KB (dépasse la limite de {max_size_kb} KB)")

    return part_files


# === Exemple d'utilisation ===
if __name__ == "__main__":
    parts = convert_then_split_parquet_dynamic("data.csv", max_size_kb=100, sample_lines=300)

