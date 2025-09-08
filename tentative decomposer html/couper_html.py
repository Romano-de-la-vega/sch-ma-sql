# pip install beautifulsoup4
from bs4 import BeautifulSoup
import html, re

def html_to_text(html_str: str) -> str:
    soup = BeautifulSoup(html_str, "html.parser")

    # Retirer scripts/styles
    for t in soup(["script", "style"]):
        t.decompose()

    # <br> -> nouvelle ligne
    for br in soup.find_all("br"):
        br.replace_with("\n")

    # Ajouter des sauts de ligne autour des blocs courants
    for tag in soup.find_all(["p", "div", "h1", "h2", "h3", "h4", "tr"]):
        tag.insert_before("\n")
        tag.append("\n")

    # Aplatir les <table> en lignes simples (une cellule par ligne)
    for table in soup.find_all("table"):
        rows = []
        for tr in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            if cells:
                rows.append(" | ".join(cells))   # remplace par "\t" si tu préfères du TSV
        table.replace_with("\n".join(rows) + "\n")

    # Récupérer le texte
    text = soup.get_text(separator="\n")

    # Dé-encoder les entités HTML et nettoyer les espaces
    text = html.unescape(text)
    text = "\n".join(line.rstrip() for line in text.splitlines())
    text = re.sub(r"\n{2,}", "\n", text).strip()

    return text

# --- Exemple d'utilisation ---
# if __name__ == "__main__":
#     html_input = """... ton gros HTML ici ..."""
#     print(html_to_text(html_input))

# pip install beautifulsoup4
from bs4 import BeautifulSoup

if __name__ == "__main__":
    # lis ton fichier HTML (si UTF-8 plante, essaie cp1252)
    with open("tentative decomposer html\mpd E715 1.html", "r", encoding="latin-1") as f:
        html_input = f.read()

    # conversion
    texte = html_to_text(html_input)

    # écriture dans un fichier de sortie
    with open("resultat.txt", "w", encoding="utf-8") as f_out:
        f_out.write(texte)

    print("✅ Extraction terminée. Résultat enregistré dans resultat.txt")

