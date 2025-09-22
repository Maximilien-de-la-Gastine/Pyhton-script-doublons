#!/usr/bin/env python3
# mp3_dup_finder_gui_single_method.py
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

def get_duration(path):
    try:
        f = MutagenFile(path)
        if not f or not f.info:
            return ""
        total_seconds = int(f.info.length)
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        return f"{minutes}:{seconds:02d}"
    except Exception:
        return ""

# ---------------- scan ----------------
def find_duplicates(root, method, progress_q=None, algo="md5"):
    files = list(iter_mp3_files(root))
    total_files = len(files)
    if progress_q:
        progress_q.put(("total", total_files))

    file_infos = []
    processed = 0
    for f in files:
        info = {"path": f}
        if method == 'hash':
            try:
                info['hash'] = file_hash(f, algo=algo)
            except Exception:
                info['hash'] = None
        elif method == 'name':
            tags = read_tags(f)
            info['title'] = (tags.get("title") or [os.path.splitext(os.path.basename(f))[0]])[0]
        elif method == 'size':
            try:
                info['size'] = os.path.getsize(f)
            except Exception:
                info['size'] = None
        file_infos.append(info)
        processed += 1
        if progress_q:
            progress_q.put(("progress", processed))

    groups = []
    used = set()
    for i, fi in enumerate(file_infos):
        if fi['path'] in used:
            continue
        group = [fi['path']]
        for j in range(i+1, len(file_infos)):
            fj = file_infos[j]
            if fj['path'] in used:
                continue
            match = True
            if method == 'hash' and fi.get('hash') != fj.get('hash'):
                match = False
            elif method == 'name' and fi.get('title') != fj.get('title'):
                match = False
            elif method == 'size' and fi.get('size') != fj.get('size'):
                match = False
            if match:
                group.append(fj['path'])
                used.add(fj['path'])
        if len(group) > 1:
            groups.append(group)
            used.update(group)

    final_groups = []
    for g in groups:
        tags = [read_tags(p) for p in g]
        final_groups.append({'files': g, 'tags': tags})
    return final_groups

def scan_worker(root, method, q, algo):
    try:
        groups = find_duplicates(root, method, progress_q=q, algo=algo)
        q.put(("done", groups))
    except Exception as e:
        q.put(("error", str(e)))

# ---------------- GUI ----------------
sg.theme("SystemDefault")

layout = [
    [sg.Text("Dossier à scanner :"), sg.Input(key="-FOLDER-"), sg.FolderBrowse()],
    [sg.Text("Méthode de détection :")],
    [sg.Radio("Hash du fichier (MD5/SHA1/SHA256)", "METHOD", key="-HASH-", default=True),
     sg.Radio("Nom du fichier / Titre", "METHOD", key="-NAME-")],
    [sg.Radio("Taille du fichier", "METHOD", key="-SIZE-")],
    [sg.Text("Algorithme de hash:"), sg.Combo(["md5","sha1","sha256"], default_value="md5", key="-ALGO-")],
    [sg.Button("Lancer le scan", key="-START-"), sg.Button("Arrêter", key="-STOP-", disabled=True)],
    [sg.ProgressBar(max_value=100, orientation='h', size=(40, 20), key="-PROG-")],
    [sg.Text("Fichiers traités: 0 / 0", key="-PROGTXT-")],
    [sg.Table(values=[],
              headings=["Titre", "Artiste", "Album", "Dossier", "Durée", "Chemin"],
              auto_size_columns=False,
              col_widths=[30,30,30,20,10,0],
              display_row_numbers=False,
              justification='left',
              key="-TABLE-",
              select_mode=sg.TABLE_SELECT_MODE_EXTENDED,
              num_rows=20)],
    [sg.Button("Exporter CSV", key="-EXPORT-"), sg.Button("Déplacer sélection", key="-MOVE-")],
    [sg.StatusBar("", key="-STATUS-", size=(80,1))]
]

window = sg.Window("Find MP3 Duplicates", layout, finalize=True)

worker_thread = None
msg_q = queue.Queue()
groups_cache = []
total_files = 0
preserve_map = {}

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
                table_values = []
                for g in groups_cache:
                    for p, t in zip(g["files"], g.get("tags", [{}])):
                        title = (t.get("title") or [""])[0]
                        artist = (t.get("artist") or [""])[0]
                        album = (t.get("album") or [""])[0]
                        folder = os.path.basename(os.path.dirname(p))
                        duration = get_duration(p)
                        table_values.append([title, artist, album, folder, duration, p])
                window["-TABLE-"].update(values=table_values)
                window["-STATUS-"].update(f"Scan terminé — {len(groups_cache)} groupes de doublons trouvés")
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
        method = None
        if values["-HASH-"]:
            method = "hash"
        elif values["-NAME-"]:
            method = "name"
        elif values["-SIZE-"]:
            method = "size"
        if not folder or not os.path.isdir(folder):
            sg.popup("Veuillez sélectionner un dossier valide.")
            continue
        if not method:
            sg.popup("Veuillez sélectionner une méthode de détection.")
            continue
        msg_q = queue.Queue()
        worker_thread = threading.Thread(target=scan_worker, args=(folder, method, msg_q, algo), daemon=True)
        worker_thread.start()
        window["-STATUS-"].update("Scan en cours...")
        window["-START-"].update(disabled=True)
        window["-STOP-"].update(disabled=False)
        total_files = 0
        window["-TABLE-"].update(values=[])
        groups_cache = []

    if event == "-STOP-":
        sg.popup("Arrêt demandé — fermez la fenêtre pour stopper.")
        window["-STATUS-"].update("Arrêt demandé (fermez la fenêtre pour forcer l'arrêt)")

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
                writer.writerow(["title","artist","album","folder","duration","path"])
                for g in groups_cache:
                    for p, t in zip(g["files"], g.get("tags", [{}])):
                        title = (t.get("title") or [""])[0]
                        artist = (t.get("artist") or [""])[0]
                        album = (t.get("album") or [""])[0]
                        folder = os.path.basename(os.path.dirname(p))
                        duration = get_duration(p)
                        writer.writerow([title, artist, album, folder, duration, p])
            sg.popup("Export terminé:", out)
        except Exception as e:
            sg.popup("Erreur export:", e)

    if event == "-MOVE-":
        if not groups_cache:
            sg.popup("Aucun groupe à traiter.")
            continue
        dest = sg.popup_get_folder("Dossier cible pour déplacer les doublons")
        if not dest:
            continue
        try:
            os.makedirs(dest, exist_ok=True)
            for g in groups_cache:
                folders = sorted(set(os.path.dirname(p) for p in g["files"]))
                preserve_folder = None
                for f in folders:
                    if f in preserve_map:
                        preserve_folder = preserve_map[f]
                        break
                if not preserve_folder and len(folders) > 1:
                    first_file = g["files"][0]
                    tags = g.get("tags", [{}])[0] if g.get("tags") else {}
                    title = (tags.get("title") or [""])[0]
                    artist = (tags.get("artist") or [""])[0]
                    album = (tags.get("album") or [""])[0]
                    duration = get_duration(first_file)
                    info_text = f"Premier doublon :\nTitre: {title}\nArtiste: {artist}\nAlbum: {album}\nDurée: {duration}\n\nChoisissez le dossier à PRESERVER :"
                    layout_choice = [
                        [sg.Text(info_text)],
                        [sg.Listbox(folders, size=(80, len(folders)), key="-CHOICE-", select_mode=sg.LISTBOX_SELECT_MODE_SINGLE)],
                        [sg.Button("OK"), sg.Button("Annuler")]
                    ]
                    win_choice = sg.Window("Sélection du dossier à préserver", layout_choice, modal=True)
                    while True:
                        e, v = win_choice.read()
                        if e in (sg.WINDOW_CLOSED, "Annuler"):
                            win_choice.close()
                            preserve_folder = None
                            break
                        if e == "OK" and v["-CHOICE-"]:
                            preserve_folder = v["-CHOICE-"][0]
                            win_choice.close()
                            remember = sg.popup_yes_no(f"Voulez-vous toujours préserver {preserve_folder} pour les futurs doublons ?")
                            if remember == "Yes":
                                for f in folders:
                                    preserve_map[f] = preserve_folder
                            break
                to_move = [p for p in g["files"] if os.path.dirname(p) != preserve_folder]
                for p in to_move:
                    fn = os.path.basename(p)
                    dst_path = os.path.join(dest, fn)
                    base, ext = os.path.splitext(dst_path)
                    c = 1
                    while os.path.exists(dst_path):
                        dst_path = f"{base}_{c}{ext}"
                        c += 1
                    shutil.move(p, dst_path)
            sg.popup("Déplacement terminé. Les doublons ont été déplacés vers:", dest)
            window["-STATUS-"].update("Déplacement terminé")
        except Exception as e:
            sg.popup("Erreur déplacement:", e)

window.close()
