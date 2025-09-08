from collections import Counter
import string

infile = "mots_plc.txt"
outfile = "classement.txt"

def clean_line(line: str) -> str:
    # garde uniquement les caractères imprimables
    return "".join(ch for ch in line if ch in string.printable).strip()

# Lire et nettoyer les mots, passage en minuscules
with open(infile, "r", encoding="utf-8", errors="ignore") as f:
    words = [clean_line(line).lower() for line in f if clean_line(line)]

# Compter les occurrences
counter = Counter(words)
items = sorted(counter.items(), key=lambda x: (-x[1], x[0]))

# Sauvegarder le classement
with open(outfile, "w", encoding="utf-8") as out:
    out.write("rang\tfreq\tmot\n")
    rank = 1
    prev_freq = None
    for i, (word, freq) in enumerate(items, start=1):
        if prev_freq is None or freq < prev_freq:
            rank = i
        prev_freq = freq
       # out.write(f"{rank}\t{freq}\t{word}\n")
        out.write(f"{word}\n")

print("✅ Fichier nettoyé, casse ignorée, écrit dans :", outfile)
