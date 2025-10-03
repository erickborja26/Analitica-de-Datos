# senamhi_hourly.py
import os, csv, time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

URL = "https://www.senamhi.gob.pe/?p=calidad-del-aire"
OUT_CSV = "senamhi_detalle_hourly.csv"
SCHEMA = ["Estacion","Fecha","Hora","PM 2,5","PM 10","SO2","NO2","O3","CO"]

def new_driver(headless=True):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1440,900")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--user-agent=Mozilla/5.0")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def wait_ready(d, t=60):
    WebDriverWait(d, t).until(lambda x: x.execute_script("return document.readyState") == "complete")

def enter_leaflet_iframe(d):
    # principal
    if d.find_elements(By.CSS_SELECTOR, ".leaflet-container"): return True
    # iframes
    for fr in d.find_elements(By.CSS_SELECTOR, "iframe"):
        try:
            d.switch_to.default_content()
            WebDriverWait(d, 10).until(EC.frame_to_be_available_and_switch_to_it(fr))
            if d.find_elements(By.CSS_SELECTOR, ".leaflet-container"): return True
        except Exception:
            pass
    d.switch_to.default_content()
    return False

def click_js(d, el):
    d.execute_script("arguments[0].dispatchEvent(new MouseEvent('click', {bubbles:true}))", el)

def get_popup_html(d, timeout=12):
    try:
        el = WebDriverWait(d, timeout).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, ".leaflet-popup-content"))
        )
        return el.get_attribute("innerHTML")
    except TimeoutException:
        return None

XPATH_ESTACION = ("//div[contains(@class,'leaflet-popup-content')]"
                  "//*[self::b or self::strong][normalize-space()='Estación:']"
                  "/ancestor::td/following-sibling::td[1]")

def extract_station_name(d, popup_html_backup=None):
    try:
        td = WebDriverWait(d, 5).until(EC.visibility_of_element_located((By.XPATH, XPATH_ESTACION)))
        name = td.text.strip()
        if name: return name
    except TimeoutException:
        pass
    if not popup_html_backup:
        popup_html_backup = get_popup_html(d, timeout=3)
    if popup_html_backup:
        soup = BeautifulSoup(popup_html_backup, "html.parser")
        b = soup.find(["b","strong"], string=lambda s: s and "Estación" in s)
        if b:
            td_label = b.find_parent("td")
            td_val = td_label.find_next_sibling("td") if td_label else None
            if td_val:
                return td_val.get_text(strip=True)
    return ""

def parse_first_row_by_position(popup_html):
    """
    Devuelve solo la PRIMERA fila (más reciente) mapeada por posición:
    Fecha, Hora, PM2.5, PM10, SO2, NO2, O3, CO
    """
    if not popup_html:
        return None
    soup = BeautifulSoup(popup_html, "html.parser")
    content2 = (soup.select_one("div.content > div.content-2")
                or soup.select_one("div.content-2")
                or soup.select_one("div.content"))
    table = content2.find("table") if content2 else None
    if not table:
        return None
    tbody = table.find("tbody")
    if not tbody:
        return None
    tr = tbody.find("tr")
    if not tr:
        return None
    tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
    if len(tds) < 2:
        return None

    # Asegura al menos 8 columnas (Fecha, Hora + 6 contaminantes)
    while len(tds) < 8:
        tds.append("")

    # normaliza decimales coma->punto
    def norm(x):
        if isinstance(x, str) and "," in x:
            try:
                float(x.replace(",", "."))
                return x.replace(",", ".")
            except:
                return x
        return x

    out = {
        "Fecha": tds[0],
        "Hora":  tds[1],
        "PM 2,5": norm(tds[2]),
        "PM 10": norm(tds[3]),
        "SO2":   norm(tds[4]),
        "NO2":   norm(tds[5]),
        "O3":    norm(tds[6]),
        "CO":    norm(tds[7]),
    }
    return out

def read_existing_keys(path):
    """Lee llaves existentes (Estacion|Fecha|Hora) para evitar duplicados."""
    keys = set()
    if not os.path.exists(path):
        return keys
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            keys.add(f"{r.get('Estacion','')}|{r.get('Fecha','')}|{r.get('Hora','')}")
    return keys

def append_rows(path, rows):
    file_exists = os.path.exists(path)
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SCHEMA)
        if not file_exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)

def run_once():
    d = new_driver(headless=True)
    d.get(URL)
    wait_ready(d, 90)

    if not enter_leaflet_iframe(d):
        print("No se encontró el iframe del mapa.")
        d.quit()
        return

    markers_css = ".leaflet-marker-pane .leaflet-marker-icon"
    resultados = []
    seen_keys = read_existing_keys(OUT_CSV)

    total = len(d.find_elements(By.CSS_SELECTOR, markers_css))
    for i in range(total):
        ms = d.find_elements(By.CSS_SELECTOR, markers_css)
        if i >= len(ms): break
        m = ms[i]

        click_js(d, m)
        html = get_popup_html(d, timeout=12)
        if not html:
            click_js(d, m); html = get_popup_html(d, timeout=12)
        if not html:
            continue

        # Mostrar/activar la pestaña con la tabla si existe
        try:
            lab = d.find_elements(By.CSS_SELECTOR, "label[for='tab-3']")
            if lab:
                click_js(d, lab[0]); time.sleep(0.25)
                html = get_popup_html(d, timeout=6) or html
        except Exception:
            pass

        estacion = extract_station_name(d, popup_html_backup=html)
        row = parse_first_row_by_position(html)
        # Cierra popup
        d.execute_script("document.dispatchEvent(new KeyboardEvent('keydown', {'key':'Escape'}));")
        time.sleep(0.15)

        if not row:
            continue
        row["Estacion"] = estacion
        key = f"{row['Estacion']}|{row['Fecha']}|{row['Hora']}"
        if key not in seen_keys:
            resultados.append({k: row.get(k, "") for k in SCHEMA})
            seen_keys.add(key)

    d.switch_to.default_content()
    d.quit()

    if resultados:
        append_rows(OUT_CSV, resultados)
        print(f"{datetime.now()} -> añadidas {len(resultados)} filas a {OUT_CSV}")
    else:
        print(f"{datetime.now()} -> no hubo filas nuevas (posible duplicado o sin datos).")

if __name__ == "__main__":
    run_once()
