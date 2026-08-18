"""
========================================================================================
BOT DE SIGNAL LIVE (RED/BLACK) : WEBSOCKET -> MACHINE A ETATS -> TELEGRAM
========================================================================================
Même infrastructure WebSocket/Telegram que les autres bots, moteur de signal
RED/BLACK "streak de répétition" (PAS chaos) :

- streak = nombre de tirages consécutifs de la MÊME couleur (Rouge ou Noir).
- Dès que streak atteint chaos_threshold, on parie sur la couleur OPPOSÉE
  (celle "en retard").
- Mise fixe sur 4 paliers : 25, 50, 75, 100 DHS (pas de Fibonacci).
- Sur une perte : on change la cible pour chasser la couleur qui vient de
  sortir. Un 0 ne change JAMAIS la cible et ne consomme pas de vie.
- Deux 0 consécutifs (en phase de guet uniquement) réinitialisent le streak.
- Un signal ne se déclenche JAMAIS sur un 0.
- Bust = perte des 4 paliers d'affilée -> recharge automatique du capital.
- ALERTE PRÉCOCE : notification Telegram dès que le streak atteint 10/11,
  une ligne avant le vrai signal — purement informative, aucune mise engagée.
- Tous les événements (alerte précoce, signal, gain, perte, bust) sont
  relayés sur Telegram sans filtre — plus de gate post-bust.

--------------------------------------------------------------------------------------
FIX (voir échange) :
  on_open() envoyait le message "Bot démarré" à CHAQUE reconnexion WebSocket,
  pas seulement au vrai démarrage du process. Un drop de connexion (fréquent
  sur ces flux WS de casino live) déclenchait donc un faux message "démarré"
  sur Telegram, qui ressemblait à un redémarrage Railway alors que ce n'en
  était pas un.

  Correctif :
    1. Un flag global `_first_connection` distingue le tout premier `on_open`
       (vrai démarrage du process) des reconnexions suivantes.
    2. Les reconnexions sont seulement loguées en console (avec un compteur),
       PAS envoyées sur Telegram, pour éviter de spammer le téléphone à
       chaque coupure WS mineure.
    3. Un compteur `_reconnect_count` permet de voir en un coup d'œil combien
       de fois la connexion a sauté depuis le démarrage du process.
--------------------------------------------------------------------------------------
========================================================================================
"""

import json
import time
import csv
import os
import requests
import websocket  # pip install websocket-client
from datetime import datetime

# ==========================================================================
# 0. ENREGISTREMENT CSV
# ==========================================================================
VOLUME_PATH = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", ".")
CSV_FILE = os.path.join(VOLUME_PATH, "roulette_data.csv")
CSV_HEADERS = ["Timestamp", "GameID", "Result", "Color"]


def load_last_game_id_from_csv():
    """Lit le dernier gameId déjà enregistré, pour rattraper les spins
    manqués pendant une coupure/redémarrage (AVANT de recréer le header)."""
    if not os.path.exists(CSV_FILE):
        return None
    try:
        with open(CSV_FILE, newline='') as f:
            rows = list(csv.reader(f))
        if len(rows) <= 1:
            return None
        return rows[-1][1]
    except Exception as e:
        print(f"[CSV] Impossible de lire le dernier gameId : {e}")
        return None


if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)


def log_spin_to_csv(game_id, result, color, spin_time=None):
    timestamp = spin_time if spin_time else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = [timestamp, game_id, result, color]
    with open(CSV_FILE, mode='a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(row)

# ==========================================================================
# 1. CONFIGURATION TELEGRAM
# ==========================================================================
TELEGRAM_BOT_TOKEN = "8690525417:AAHwU0unxGyXnl5YgcyDigIwpKS7nU-3Cmk"
TELEGRAM_CHAT_ID = "6098394153"

def send_telegram_alert(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"[Telegram] Erreur d'envoi : {e}")


# ==========================================================================
# 2. CONFIGURATION WEBSOCKET + PROXY RÉSIDENTIEL
# ==========================================================================
WS_URL = "wss://dga.pragmaticplaylive.net/ws"

TABLE_KEY = "2244"
CURRENCY = "EUR"
CASINO_ID = "il9srgw4dna22222"

WS_HEADERS = [
    "Accept-Encoding: gzip, deflate, br, zstd",
    "Accept-Language: en-US,en;q=0.9,fr-MA;q=0.8,fr;q=0.7",
    "Cache-Control: no-cache",
    "Pragma: no-cache",
    "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",
]
WS_ORIGIN = "https://www.bigwinboard.com"

PROXY_HOST = "gw.dataimpulse.com"
PROXY_PORT = 824
PROXY_TYPE = "socks5"
PROXY_LOGIN = "c28464d2322ae2cb5a09"
PROXY_PASSWORD = "afd6703a49960bd1"
USE_PROXY = True

DEBUG_TRACE = False


# ==========================================================================
# 3. LA MACHINE A ETATS "EN LIGNE" — RED/BLACK, PALIERS FIXES
# ==========================================================================
RED_NUMBERS = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}

def get_color(n):
    if n == 0:
        return 0
    return 1 if n in RED_NUMBERS else 2  # 1 = Rouge, 2 = Noir


class LiveSignalEngine:
    def __init__(self, initial_capital, chaos_threshold=11, target_wins=1,
                 bet_ladder=(25, 50, 75, 100)):
        self.bet_ladder = bet_ladder
        self.lives = len(bet_ladder)
        self.actual_required_capital = sum(bet_ladder)  # 250 DHS d'exposition max

        self.chaos_threshold = chaos_threshold
        self.target_wins = target_wins

        self.capital = initial_capital
        self.initial_capital = initial_capital
        self.total_real_deposits = initial_capital

        self.is_betting = False
        self.life_index = 0
        self.target_color = None
        self.wins_in_current_signal = 0
        self.current_sequence_loss = 0

        self.streak_color = 0
        self.consecutive_zeros = 0

        self.p_col = None

        self.signal_counter = 0

    def process_spin(self, number):
        events = []
        c_col = get_color(number)

        if self.p_col is None:
            self.p_col = c_col
            return events

        # -------------------------------------------------------------
        # PHASE DE GUET
        # -------------------------------------------------------------
        if not self.is_betting:
            if c_col == 0:
                self.consecutive_zeros += 1
                if self.consecutive_zeros >= 2:
                    self.streak_color = 0
                    self.consecutive_zeros = 0
                self.p_col = c_col
                return events  # un 0 ne compte jamais comme déclencheur
            else:
                self.consecutive_zeros = 0

            if c_col == self.p_col:
                self.streak_color += 1
            else:
                self.streak_color = 1

            # 🔔 ALERTE PRÉCOCE : se déclenche dès que le streak atteint 10
            # (chaos_threshold - 1), une ligne avant le vrai signal à 11.
            # Purement informative — aucune mise n'est engagée ici.
            if self.streak_color == self.chaos_threshold - 1:
                label = "ROUGE" if c_col == 1 else "NOIR"
                events.append(
                    f"⏳ <b>ALERTE PRÉCOCE</b> — {label} approche du seuil "
                    f"({self.streak_color}/{self.chaos_threshold}). Prépare-toi, "
                    f"le signal peut se déclencher au prochain spin."
                )

            if self.streak_color >= self.chaos_threshold:
                self.target_color = 1 if c_col == 2 else 2  # couleur opposée
                self.is_betting = True
                self.life_index = 0
                self.wins_in_current_signal = 0
                self.streak_color = 0
                self.current_sequence_loss = 0
                self.signal_counter += 1

                label = "ROUGE" if self.target_color == 1 else "NOIR"
                events.append(
                    f"⚡ <b>SIGNAL #{self.signal_counter}</b> — {label}\n"
                    f"Mise à placer : <b>{self.bet_ladder[0]} DHS</b> sur {label}"
                )

            self.p_col = c_col
            return events

        # -------------------------------------------------------------
        # PHASE D'ATTAQUE
        # -------------------------------------------------------------
        bet_amount = self.bet_ladder[self.life_index]
        actual_color = c_col

        if actual_color == 0:
            # Un 0 ne change pas la cible et ne consomme pas de vie
            self.p_col = c_col
            return events

        if actual_color == self.target_color:
            net_gain = bet_amount * 2
            self.capital += net_gain
            total_money_used = self.current_sequence_loss + bet_amount
            profit = net_gain - total_money_used

            self.wins_in_current_signal += 1
            events.append(
                f"🟢 <b>GAIN</b> — Séquence #{self.signal_counter} | Palier {self.life_index + 1}\n"
                f"Profit : +{profit} DHS | Capital : {self.capital} DHS"
            )

            self.life_index = 0
            self.current_sequence_loss = 0

            if self.wins_in_current_signal >= self.target_wins:
                self.is_betting = False
                events.append(f"✅ Signal #{self.signal_counter} terminé — objectif atteint.")
        else:
            self.capital -= bet_amount
            self.current_sequence_loss += bet_amount
            self.life_index += 1
            self.target_color = actual_color  # on chasse la couleur qui vient de sortir

            next_bet = self.bet_ladder[self.life_index] if self.life_index < self.lives else "BUST"
            events.append(
                f"🔴 Perte palier {self.life_index} | Prochaine mise : {next_bet} DHS"
            )

        if self.life_index >= self.lives:
            solde_restant = self.capital
            if solde_restant < self.actual_required_capital:
                apport = self.initial_capital - solde_restant
                self.total_real_deposits += apport
                self.capital = self.initial_capital
                events.append(
                    f"🚨 <b>BUST</b> — Recharge de {apport} DHS nécessaire. "
                    f"Capital remis à {self.initial_capital} DHS."
                )
            else:
                events.append(f"🚨 Fin de séquence — capital auto-suffisant ({solde_restant} DHS).")

            self.is_betting = False
            self.life_index = 0
            self.current_sequence_loss = 0
            self.streak_color = 0
            self.consecutive_zeros = 0

        self.p_col = c_col
        return events


# ==========================================================================
# 4. CLIENT WEBSOCKET
# ==========================================================================
engine = LiveSignalEngine(
    initial_capital=1000,
    chaos_threshold=11,
    target_wins=1,
    bet_ladder=(25, 50, 75, 100),
)

last_game_id = load_last_game_id_from_csv()
if last_game_id:
    print(f"[Rattrapage] Dernier gameId connu au démarrage : {last_game_id}")


def handle_new_result(number, table_id):
    t0 = time.time()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Nouveau spin (table {table_id}) : {number}")
    events = engine.process_spin(number)
    t1 = time.time()
    for msg in events:
        print(msg)  # toujours visible en console/logs
        send_telegram_alert(msg)  # tout est relayé sur Telegram, sans filtre
    t2 = time.time()
    print(f"[TIMING] engine={t1-t0:.3f}s | telegram={t2-t1:.3f}s | total={t2-t0:.3f}s")


def process_new_results(results):
    global last_game_id

    if last_game_id is None:
        new_entries = list(reversed(results))
    else:
        idx = next((i for i, r in enumerate(results) if r.get("gameId") == last_game_id), None)
        if idx is None:
            print("[Rattrapage] ⚠️ Coupure trop longue (>20 spins) — rattrapage partiel impossible, reprise au plus récent.")
            new_entries = [results[0]] if results else []
        elif idx == 0:
            new_entries = []
        else:
            new_entries = list(reversed(results[:idx]))
            if len(new_entries) > 1:
                print(f"[Rattrapage] {len(new_entries)} spin(s) manqué(s) détecté(s), traitement en cours...")

    for entry in new_entries:
        game_id = entry.get("gameId")
        if game_id is None:
            continue

        last_game_id = game_id

        t_csv0 = time.time()
        log_spin_to_csv(game_id, entry.get("result"), entry.get("color"), entry.get("time"))
        t_csv1 = time.time()
        if t_csv1 - t_csv0 > 0.1:
            print(f"[TIMING] écriture CSV lente : {t_csv1 - t_csv0:.3f}s")

        try:
            number = int(entry["result"])
        except (KeyError, ValueError, TypeError):
            continue

        handle_new_result(number, TABLE_KEY)


def on_message(ws, message):
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        return

    if str(data.get("tableId")) != TABLE_KEY:
        return

    results = data.get("last20Results")
    if not results:
        return

    process_new_results(results)


def on_error(ws, error):
    print(f"[WS] Erreur : {repr(error)} (type: {type(error).__name__})")


def on_close(ws, close_status_code, close_msg):
    print(f"[WS] Connexion fermée (code={close_status_code}, msg={close_msg}). Reconnexion dans 3s...")


# --------------------------------------------------------------------------
# FIX : distinguer le vrai démarrage du process d'une simple reconnexion WS.
# --------------------------------------------------------------------------
_first_connection = True
_reconnect_count = 0


def on_open(ws):
    global _first_connection, _reconnect_count

    print("[WS] Connexion établie.")

    if _first_connection:
        send_telegram_alert(
            f"🎲 Bot RED/BLACK démarré (WebSocket). Capital : {engine.initial_capital} DHS, "
            f"seuil : {engine.chaos_threshold}, paliers : {engine.bet_ladder}.\n"
            f"Telegram : tous les signaux sont relayés (pas de filtre)."
        )
        _first_connection = False
    else:
        _reconnect_count += 1
        # Reconnexion normale (drop WS courant sur ce type de flux) :
        # on log en console mais on NE spamme PAS Telegram à chaque coupure.
        print(f"[WS] Reconnexion #{_reconnect_count} effectuée (pas un redémarrage du process).")

    msg1 = json.dumps({"type": "available", "casinoId": CASINO_ID})
    ws.send(msg1)
    print(f"[WS] Message 'available' envoyé : {msg1}")

    time.sleep(1)

    msg2 = json.dumps({"type": "subscribe", "currency": CURRENCY, "key": TABLE_KEY, "casinoId": CASINO_ID})
    ws.send(msg2)
    print(f"[WS] Message 'subscribe' envoyé : {msg2}")


def run_forever_with_reconnect():
    if DEBUG_TRACE:
        import logging
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')
        websocket.enableTrace(True)

    while True:
        ws = websocket.WebSocketApp(
            WS_URL,
            header=WS_HEADERS,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        run_kwargs = {"ping_interval": 30, "ping_timeout": 25, "origin": WS_ORIGIN}
        if USE_PROXY:
            run_kwargs.update({
                "http_proxy_host": PROXY_HOST,
                "http_proxy_port": PROXY_PORT,
                "http_proxy_auth": (PROXY_LOGIN, PROXY_PASSWORD),
                "proxy_type": PROXY_TYPE,
            })

        try:
            ws.run_forever(**run_kwargs)
        except Exception as e:
            print(f"[WS] Exception : {repr(e)} (type: {type(e).__name__})")

        print("[WS] Reconnexion dans 3 secondes...")
        time.sleep(3)


if __name__ == "__main__":
    if VOLUME_PATH == ".":
        print(f"⚠️ ATTENTION : RAILWAY_VOLUME_MOUNT_PATH non détecté — CSV éphémère utilisé : {os.path.abspath(CSV_FILE)}")
    else:
        print(f"✅ Volume détecté ({VOLUME_PATH}) — CSV persistant utilisé : {CSV_FILE}")
    print(f"🎲 Bot RED/BLACK démarré | Paliers : {engine.bet_ladder} | Exposition max/signal : {engine.actual_required_capital} DHS")
    run_forever_with_reconnect()
