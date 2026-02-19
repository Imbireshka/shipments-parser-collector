"""
Multi-PVS Data Collector (Parser)

This script demonstrates:
- Concurrent scraping of multiple PVS (Pickup Points) instances
- Grouping shipments by time windows
- Extracting detailed metrics from nested pages
- Saving structured data to PostgreSQL
- Sending reports via Telegram

"""

import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Disable SSL warnings (use cautiously in production)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === Configuration ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS", "").split(",")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "shipments_db"),
    "user": os.getenv("DB_USER", "collector"),
    "password": os.getenv("DB_PASSWORD"),
}

# Generic credentials for demo/login
USERNAME = os.getenv("PVS_USERNAME", "demo_user")
PASSWORD = os.getenv("PVS_PASSWORD", "demo_pass")

# List of PVS instance identifiers (e.g., location codes)
PVS_IDENTIFIERS = os.getenv("PVS_LIST", "site1,site2,site3").split(",")


def send_telegram_message(message: str):
    """Send message to Telegram via direct IP to bypass DNS issues."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_IDS[0]:
        print("Чат айди не найден, пропуск отсылки отчета")
        return

    telegram_ip = "149.154.167.220"  # api.telegram.org
    url = f"https://{telegram_ip}/bot{TELEGRAM_TOKEN}/sendMessage"
    headers = {"Host": "api.telegram.org"}

    for chat_id in TELEGRAM_CHAT_IDS:
        try:
            payload = {
                "chat_id": chat_id.strip(),
                "text": message,
                "disable_web_page_preview": True,
            }
            response = requests.post(url, data=payload, headers=headers, verify=False, timeout=10)
            if response.status_code != 200:
                print(f"Telegram error for {chat_id}: {response.status_code}")
        except Exception as e:
            print(f"Ошибка отправки в Телеграмм: {e}")


def save_group_to_db(pvs_name: str, group: list, group_index: int, today_date: str, count: int, unload_duration_seconds: int):
    """Save shipment group to PostgreSQL."""
    conn = None
    try:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_id = f"GROUP_{run_id}_{pvs_name}_{group_index}"

        total_sent = sum(s.get("sent", 0) for s in group)
        total_received = sum(s.get("received", 0) for s in group)
        total_excess = sum(s.get("excess", 0) for s in group)

        def safe_parse(dt_str):
            try:
                return datetime.strptime(dt_str.strip(), "%Y-%m-%d %H:%M:%S")
            except:
                return None

        created_at = min(s["created_dt"] for s in group)
        start_times = [safe_parse(s["unload_started_at"]) for s in group if s.get("unload_started_at", "-") != "-"]
        unload_started_at = min(t for t in start_times if t) if start_times else None

        statuses = [s.get("status", "").lower() for s in group]
        closed_at = None
        if all(s == "closed" for s in statuses):
            close_times = [safe_parse(s["closed_at"]) for s in group if s.get("closed_at", "-") != "-"]
            closed_at = max(t for t in close_times if t) if close_times else None

        if any(s == "in_progress" for s in statuses):
            status = "in_progress"
        elif all(s == "pending" for s in statuses):
            status = "pending"
        else:
            status = "closed"

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO shipments (
                report_id, pvs_name, delivery_date, created_at,
                unload_started_at, closed_at, status,
                sent, received, excess, group_index,
                boxes_count, unload_duration_seconds
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            report_id, pvs_name, today_date, created_at,
            unload_started_at, closed_at, status,
            total_sent, total_received, total_excess,
            group_index, count, unload_duration_seconds
        ))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Ошибка БД {pvs_name} (group {group_index}): {e}")
    finally:
        if conn:
            conn.close()


def parse_datetime(date_str: str):
    try:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d %H:%M:%S")
    except:
        return None


def get_details_from_detail_page(session, detail_url: str):
    """Extract numeric metrics from a shipment detail page."""
    try:
        response = session.get(detail_url, verify=False, timeout=10)
        if response.status_code != 200:
            return None
        soup = BeautifulSoup(response.text, "html.parser")
        packs_section = soup.find("div", id="packsInfoContainer")
        if not packs_section:
            return None

        def extract_value(label_keyword: str):
            dt = packs_section.find("dt", string=lambda x: x and label_keyword in x)
            if dt:
                dd = dt.find_next_sibling("dd")
                if dd:
                    text = dd.get_text(strip=True)
                    digits = ''.join(filter(str.isdigit, text))
                    return int(digits) if digits else 0
            return 0

        return {
            "sent": extract_value("Sent"),
            "received": extract_value("Received"),
            "excess": extract_value("Excess")
        }
    except Exception as e:
        print(f"Ошибка обработки детальной страницы ({detail_url}): {e}")
        return None


def process_pvs(pvs_id: str):
    """Process one PVS instance."""
    base_url = f"https://example-pvs-{pvs_id}.local"
    login_url = f"{base_url}/user/login"
    data_url = f"{base_url}/shipments/incoming/"

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; PVS-Collector/1.0)"})

    try:
        # Login
        login_page = session.get(login_url, verify=False, timeout=15)
        if login_page.status_code != 200:
            return {"error": f"Login page unreachable ({login_page.status_code})"}

        soup = BeautifulSoup(login_page.text, "html.parser")
        redirect_input = soup.find("input", {"name": "redirect"})
        redirect_value = redirect_input["value"] if redirect_input else "/shipments/incoming/"

        payload = {
            "identity": USERNAME,
            "credential": PASSWORD,
            "redirect": redirect_value
        }
        response = session.post(login_url, data=payload, allow_redirects=True, verify=False, timeout=15)
        if "identity" in response.text and "credential" in response.text:
            return {"error": "Login failed"}

        # Scrape today's shipments
        all_shipments = []
        page = 1
        today_date = datetime.now().strftime("%Y-%m-%d")

        while True:
            url = f"{data_url}page/{page}/order_by/createdAt/desc/" if page > 1 else data_url
            try:
                response = session.get(url, verify=False, timeout=10)
                if response.status_code != 200:
                    break
                soup = BeautifulSoup(response.text, "html.parser")
                rows = soup.select("table#list-table tbody tr")
                if not rows:
                    break

                new_rows_found = False
                for row in rows:
                    created_cell = row.find("td", class_="cell-createdAt")
                    if not created_cell:
                        continue
                    created_text = created_cell.get_text(strip=True)
                    if not created_text.startswith(today_date):
                        continue

                    external_id_cell = row.find("td", class_="cell-externalId")
                    shipment_id = external_id_cell.find("a").get_text(strip=True) if external_id_cell and external_id_cell.find("a") else "—"
                    detail_url = base_url + external_id_cell.find("a")["href"] if external_id_cell and external_id_cell.find("a") else None

                    status = row.find("td", class_="cell-status").get_text(strip=True).lower()
                    closed_at = "-"
                    if status == "closed":
                        closed_elem = row.find("td", class_="cell-closedAt")
                        closed_at = closed_elem.get_text(strip=True) if closed_elem else "-"

                    unload_started_at = row.find("td", class_="cell-unloadStartedAt").get_text(strip=True)
                    created_dt = parse_datetime(created_text)
                    if not created_dt:
                        continue

                    all_shipments.append({
                        "id": shipment_id,
                        "created_dt": created_dt,
                        "created_at": created_text,
                        "unload_started_at": unload_started_at,
                        "closed_at": closed_at,
                        "status": status,
                        "detail_url": detail_url
                    })
                    new_rows_found = True

                if not new_rows_found and page > 1:
                    break
                if len(rows) < 20:
                    break
                page += 1
                time.sleep(0.1)
            except Exception as e:
                print(f"Ошибка обработки {page} ошибка: {e}")
                break

        if not all_shipments:
            return {"error": "No shipments found for today"}

        valid_shipments = [s for s in all_shipments if s["created_dt"] is not None]
        if not valid_shipments:
            return {"error": "No valid shipment timestamps"}

        valid_shipments.sort(key=lambda x: x["created_dt"])

        # Fetch details concurrently
        details_map = {}
        def fetch_details(shipment):
            if not shipment["detail_url"]:
                return shipment["id"], {"sent": 0, "received": 0, "excess": 0}
            details = get_details_from_detail_page(session, shipment["detail_url"])
            return shipment["id"], details or {"sent": 0, "received": 0, "excess": 0}

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_id = {executor.submit(fetch_details, s): s["id"] for s in valid_shipments}
            for future in as_completed(future_to_id):
                shipment_id, details = future.result()
                details_map[shipment_id] = details

        for shipment in valid_shipments:
            details = details_map[shipment["id"]]
            shipment.update(details)

        # Group shipments: night (0–8h) + day (hourly windows)
        groups = []
        night = [s for s in valid_shipments if s["created_dt"].hour < 8]
        day = [s for s in valid_shipments if s["created_dt"].hour >= 8]

        if night:
            groups.append(sorted(night, key=lambda x: x["created_dt"]))

        if day:
            current = [day[0]]
            for i in range(1, len(day)):
                if day[i]["created_dt"] - current[-1]["created_dt"] <= timedelta(hours=1):
                    current.append(day[i])
                else:
                    groups.append(current)
                    current = [day[i]]
            if current:
                groups.append(current)

        groups.sort(key=lambda g: g[-1]["created_dt"], reverse=True)

        # Build report
        lines = [f"📍 ПВЗ: {pvs_id}\n"]
        for idx, group in enumerate(groups, 1):
            start_times = [parse_datetime(s["unload_started_at"]) for s in group if s["unload_started_at"] != "-"]
            earliest_start = min(start_times) if start_times else None

            statuses = [g["status"] for g in group]
            if "in_progress" in statuses:
                group_status = "in_progress"
                latest_close = None
            elif all(s == "pending" for s in statuses):
                group_status = "pending"
                latest_close = None
            else:
                group_status = "closed"
                close_times = [parse_datetime(g["closed_at"]) for g in group if g["closed_at"] != "-"]
                latest_close = max(close_times) if close_times else None

            duration_seconds = 0
            if group_status == "closed" and earliest_start and latest_close:
                duration = latest_close - earliest_start
                duration_seconds = int(duration.total_seconds())
                duration_str = str(duration).split('.')[0]
            else:
                now = datetime.now()
                duration = now - earliest_start if earliest_start else timedelta(0)
                duration_str = str(duration).split('.')[0]

            first_created = min(g["created_dt"] for g in group).strftime("%H:%M")
            last_created = max(g["created_dt"] for g in group).strftime("%H:%M")

            total_sent = sum(s["sent"] for s in group)
            total_received = sum(s["received"] for s in group)
            total_excess = sum(s["excess"] for s in group)
            count = len(group)

            lines.append(
                f"Group: {idx}\n"
                f"Date: {today_date}\n"
                f"Arrival window: {first_created} – {last_created}\n"
                f"Closed at: {latest_close.strftime('%Y-%m-%d %H:%M:%S') if latest_close else '-'}\n"
                f"Unload started: {earliest_start.strftime('%Y-%m-%d %H:%M:%S') if earliest_start else '-'}\n"
                f"Unload duration: {duration_str}\n"
                f"Status: {group_status.capitalize()}\n"
                f"Boxes: {count}\n"
                f"Sent: {total_sent}\n"
                f"Received: {total_received}\n"
                f"Excess: {total_excess}\n"
            )

            save_group_to_db(pvs_id, group, idx, today_date, count, duration_seconds)

        return {"type": "report", "message": "".join(lines)}

    except Exception as e:
        return {"error": f"Critical error: {str(e)}"}


if __name__ == "__main__":
    print(f"Парсинг {len(PVS_IDENTIFIERS)} ПВЗ...")
    all_messages = []

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(process_pvs, pvs): pvs for pvs in PVS_IDENTIFIERS}
        for future in as_completed(futures):
            pvs = futures[future]
            try:
                result = future.result()
                if "error" in result:
                    all_messages.append(f"Ошибка {pvs}: {result['error']}")
                else:
                    all_messages.append(result["message"])
            except Exception as e:
                all_messages.append(f"{pvs}: Unhandled error — {e}")

    for msg in all_messages:
        send_telegram_message(msg)

    print("Все отчеты отправлены в Телеграмм")
