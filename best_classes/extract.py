# extract_plc_words.py
import re
import sys
import argparse

def iter_plc_words(paths, ignore_case=True):
    # capture tout ce qui suit "plc." jusqu'à l'espace
    flags = re.IGNORECASE if ignore_case else 0
    pat = re.compile(r'\bplc\.([^\s\(\)\,\\{\}\.\;\']+)', flags)
    for path in paths:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                for m in pat.finditer(line):
                    yield m.group(1)

def main():
    ap = argparse.ArgumentParser(description='Récupère les mots qui suivent "plc."')
    ap.add_argument("files", nargs="+", help="Fichiers texte à analyser")
    ap.add_argument("--unique", action="store_true", help="N’afficher que les valeurs uniques")
    ap.add_argument("--no-ignore-case", action="store_true", help="Respecter la casse (par défaut: insensible)")
    args = ap.parse_args()

    words = iter_plc_words(args.files, ignore_case=not args.no_ignore_case)
    if args.unique:
        seen = set()
        for w in words:
            if w not in seen:
                print(w)
                seen.add(w)
    else:
        for w in words:
            print(w)

if __name__ == "__main__":
    main()
