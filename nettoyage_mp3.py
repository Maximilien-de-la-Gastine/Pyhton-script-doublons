#!/usr/bin/env python3
# mp3_dup_finder_gui.py
# Requirements: pip install PySimpleGUI mutagen

import os
import hashlib
import threading
import queue
import csv
import shutil
from collections import defaultdict
import PySimpleGUI as sg
from mutagen import File as MutagenFile

# ---------------- utils ----------------
def iter_mp3_files(root):
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.lower().endswith(".mp3"):
                yield os.path.join(dirpath, fn)

def file_hash(path, algo="md5", block_size=4*1024*1024):
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        while True:
            data = f.read(block_size)
            if not data:
                break
            h.update(data)
    return h.hexdigest()

def read_tags(path):
    try:
        f = MutagenFile(path, easy=True)
        if not f:
            return {}
        return {k: v for k, v in f.items()}
    except Exception:
        return {}

def find_duplicates(root, progress_q=None, algo="md5"):
    size_map = defaultdict(list)
    files = list(iter_mp3_files(root))
    for p in files:
        try:
            sz = os.path.getsize(p)
        except OSError:
            continue
        size_map[sz].append(p)

    total_files = len(files)
    if progress_q:
        progress_q.put(("total", total_files))

    hash_map = defaultdict(list)
    processed = 0
    for sz, paths in size_map.items():
        if len(paths) == 1:
            processed += len(paths)
            if progress_q:
                progress_q.put(("progress", processed))
            continue
        for p in paths:
            try:
                h = file_hash(p, algo=algo)
                hash_map[(sz, h)].append(p)
            except Exception:
                pass
            processed += 1
            if progress_q:
                progress_q.put(("progress", processed))

    groups = []
    for (sz, h), paths in hash_map.items():
        if len(paths) > 1:
            groups.append({"size": sz, "hash": h, "files": sorted(paths)})
    for g in groups:
        g["tags"] = [read_tags(p) for p in g["files"]]
    return groups

def scan_worker(root, q, algo):
    try:
        groups = find_duplicates(root, progress_q=q, algo=algo)
        q.put(("done", groups))
    except Exception as e:
        q.put(("error", str(e)))

# ---------------- GUI ----------------
sg.theme("SystemDefault")

layout = [
    [sg.Text("Dossier à scanner :"), sg.Input(key="-FOLDER-"), sg.FolderBrowse()],
    [sg.Text("Algorithme de hash:"), sg.Combo(["md5","sha1","sha256"], default_value="md5", key="-ALGO-")],
    [sg.Button("Lancer le scan", key="-START-"), sg.Button("Arrêter", key="-STOP-", disabled=True)],
    [sg.ProgressBar(max_value=100, orientation='h', size=(40, 20), key="-PROG-")],
    [sg.Text("Fichiers traités: 0 / 0", key="-PROGTXT-")],
    [sg.Listbox(values=[], size=(100, 10), key="-GROUPS-", enable_events=True)],
    [sg.Button("Afficher fichiers du groupe", key="-SHOW-"), sg.Button("Exporter CSV", key="-EXPORT-"), sg.Button("Déplacer doublons", key="-MOVE-")],
    [sg.StatusBar("", key="-STATUS-", size=(80,1))]
]

window = sg.Window("Find MP3 Duplicates", layout, finalize=True)

worker_thread = None
msg_q = queue.Queue()
total_files = 0
groups_cache = []

def update_progress_bar(progress, total):
    frac = int(progress / total * 100) if total else 0
    window["-PROG-"].update(frac)
    window["-PROGTXT-"].update(f"Fichiers traités: {progress} / {total}")

# ---------------- main loop ----------------
while True:
    event, values = window.read(timeout=200)

    # --- handle progress queue ---
    try:
        while True:
            msg = msg_q.get_nowait()
            typ = msg[0]
            if typ == "total":
                total_files = msg[1]
            elif typ == "progress":
                update_progress_bar(msg[1], total_files)
            elif typ == "done":
                groups_cache = msg[1]
                lines = [
                    f"{os.path.basename(p)} (taille={g['size']} octets)"
                    for g in groups_cache
                    for p in g["files"]
                ]
                window["-GROUPS-"].update(lines)
                window["-STATUS-"].update(f"Scan terminé — {len(groups_cache)} groupes trouvés")
                window["-START-"].update(disabled=False)
                window["-STOP-"].update(disabled=True)
            elif typ == "error":
                window["-STATUS-"].update("Erreur: " + str(msg[1]))
                window["-START-"].update(disabled=False)
                window["-STOP-"].update(disabled=True)
    except queue.Empty:
        pass

    # --- window events ---
    if event == sg.WINDOW_CLOSED:
        break

    if event == "-START-":
        folder = values["-FOLDER-"]
        algo = values["-ALGO-"]
        if not folder or not os.path.isdir(folder):
            sg.popup("Veuillez sélectionner un dossier valide.")
            continue
        msg_q = queue.Queue()
        worker_thread = threading.Thread(target=scan_worker, args=(folder, msg_q, algo), daemon=True)
        worker_thread.start()
        window["-STATUS-"].update("Scan en cours...")
        window["-START-"].update(disabled=True)
        window["-STOP-"].update(disabled=False)
        total_files = 0
        window["-GROUPS-"].update([])
        groups_cache = []

    if event == "-STOP-":
        sg.popup("Arrêt demandé — fermez la fenêtre pour stopper.")
        window["-STATUS-"].update("Arrêt demandé (fermez la fenêtre pour forcer l'arrêt)")

    if event == "-SHOW-":
        sel = values["-GROUPS-"]
        if not sel:
            sg.popup("Sélectionnez un fichier.")
            continue
        sg.popup_scrolled("\n".join(sel), title="Fichiers du groupe", size=(100,30))

    if event == "-EXPORT-":
        if not groups_cache:
            sg.popup("Aucun résultat à exporter.")
            continue
        out = sg.popup_get_file("Nom du fichier CSV", save_as=True,
                                file_types=(("CSV Files","*.csv"),),
                                default_extension=".csv")
        if not out:
            continue
        try:
            with open(out, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow(["title","artist","album","folder"])
                for g in groups_cache:
                    for p, t in zip(g["files"], g.get("tags", [{}])):
                        title = (t.get("title") or [""])[0]
                        artist = (t.get("artist") or [""])[0]
                        album = (t.get("album") or [""])[0]
                        folder = os.path.basename(os.path.dirname(p))
                        writer.writerow([title, artist, album, folder])
            sg.popup("Export terminé:", out)
        except Exception as e:
            sg.popup("Erreur export:", e)

    if event == "-MOVE-":
        if not groups_cache:
            sg.popup("Aucun fichier à déplacer.")
            continue
        dest = sg.popup_get_folder("Dossier cible pour déplacer doublons")
        if not dest:
            continue
        try:
            os.makedirs(dest, exist_ok=True)
            for g in groups_cache:
                to_move = g["files"][1:]  # garder le premier fichier
                for p in to_move:
                    fn = os.path.basename(p)
                    dst_path = os.path.join(dest, fn)
                    base, ext = os.path.splitext(dst_path)
                    c = 1
                    while os.path.exists(dst_path):
                        dst_path = f"{base}_{c}{ext}"
                        c += 1
                    shutil.move(p, dst_path)
            sg.popup("Déplacement terminé.")
        except Exception as e:
            sg.popup("Erreur déplacement:", e)

window.close()
