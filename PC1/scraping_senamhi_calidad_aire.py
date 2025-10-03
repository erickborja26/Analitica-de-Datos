# step4b_estaciones_tabla_por_posicion.py
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import pandas as pd
import time

URL = "https://www.senamhi.gob.pe/?p=calidad-del-aire"

SCHEMA = ["Estacion","Fecha","Hora","PM 2,5","PM 10","SO2","NO2","O3","CO"]
ORDER_AFTER_TIME = ["PM 2,5","PM 10","SO2","NO2","O3","CO"]  # orden fijo por posición

# ---------------- Selenium helpers ----------------
def new_driver(headless=False):
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
    if d.find_elements(By.CSS_SELECTOR, ".leaflet-container"): return True
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

# ---------------- Parsing helpers ----------------
XPATH_ESTACION = ("//div[contains(@class,'leaflet-popup-content')]"
                  "//*[self::b or self::strong][normalize-space()='Estación:']"
                  "/ancestor::td/following-sibling::td[1]")

def extract_station_name(d, popup_html_backup=None):
    try:
        td = WebDriverWait(d, 6).until(EC.visibility_of_element_located((By.XPATH, XPATH_ESTACION)))
        name = td.text.strip()
        if name: return name
    except TimeoutException:
        pass
    if not popup_html_backup:
        popup_html_backup = get_popup_html(d, timeout=4)
    if popup_html_backup:
        soup = BeautifulSoup(popup_html_backup, "html.parser")
        b = soup.find(["b","strong"], string=lambda s: s and "Estación" in s)
        if b:
            td_label = b.find_parent("td")
            td_val = td_label.find_next_sibling("td") if td_label else None
            if td_val:
                return td_val.get_text(strip=True)
    return ""

def parse_table_by_position(popup_html):
    """Lee la tabla y asigna columnas por POSICIÓN (tras Fecha y Hora)."""
    if not popup_html:
        return []

    soup = BeautifulSoup(popup_html, "html.parser")
    # zona de tabla
    content2 = soup.select_one("div.content > div.content-2") or soup.select_one("div.content-2") or soup.select_one("div.content")
    table = content2.find("table") if content2 else None
    if not table:
        return []

    rows_out = []
    tbody = table.find("tbody")
    if not tbody:
        return rows_out

    for tr in tbody.find_all("tr"):
        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if not tds or all(x == "" for x in tds):
            continue

        # asegurar longitud mínima
        while len(tds) < 2 + len(ORDER_AFTER_TIME):
            tds.append("")

        fila = {
            "Fecha": tds[0] if len(tds) > 0 else "",
            "Hora":  tds[1] if len(tds) > 1 else "",
        }

        # a partir de pos 2, mapeo por índice fijo
        for j, col in enumerate(ORDER_AFTER_TIME, start=2):
            val = tds[j] if j < len(tds) else ""
            # normaliza decimales con coma -> punto (opcional)
            if isinstance(val, str) and "," in val and val.replace(",","").replace(".","").replace("-","").isdigit():
                val = val.replace(",", ".")
            fila[col] = val

        rows_out.append(fila)

    return rows_out

# ---------------- Main ----------------
def main():
    d = new_driver(headless=False)  # pon True cuando quieras correr en headless
    d.get(URL)
    wait_ready(d, 90)

    if not enter_leaflet_iframe(d):
        print("No se encontró el iframe del mapa.")
        d.quit()
        return

    markers_css = ".leaflet-marker-pane .leaflet-marker-icon"
    resultados = []
    total = len(d.find_elements(By.CSS_SELECTOR, markers_css))

    for i in range(total):
        ms = d.find_elements(By.CSS_SELECTOR, markers_css)
        if i >= len(ms): break
        m = ms[i]

        click_js(d, m)
        html = get_popup_html(d, timeout=12)
        if not html:
            click_js(d, m)
            html = get_popup_html(d, timeout=12)
        if not html:
            continue

        # Asegurar que la pestaña de la tabla esté visible (tab-3 suele ser la de datos)
        try:
            lab = d.find_elements(By.CSS_SELECTOR, "label[for='tab-3']")
            if lab:
                click_js(d, lab[0]); time.sleep(0.25)
                html = get_popup_html(d, timeout=6) or html
        except Exception:
            pass

        estacion = extract_station_name(d, popup_html_backup=html)
        filas = parse_table_by_position(html)

        for f in filas:
            resultados.append({
                "Estacion": estacion,
                "Fecha": f.get("Fecha",""),
                "Hora": f.get("Hora",""),
                "PM 2,5": f.get("PM 2,5",""),
                "PM 10": f.get("PM 10",""),
                "SO2":    f.get("SO2",""),
                "NO2":    f.get("NO2",""),
                "O3":     f.get("O3",""),
                "CO":     f.get("CO",""),
            })

        # cierra popup
        d.execute_script("document.dispatchEvent(new KeyboardEvent('keydown', {'key':'Escape'}));")
        time.sleep(0.2)

    d.switch_to.default_content()
    d.quit()

    df = pd.DataFrame(resultados, columns=SCHEMA)
    df.to_csv("senamhi_detalle.csv", index=False, encoding="utf-8-sig")
    print(f"Listo: {len(df)} filas guardadas en senamhi_detalle.csv")

if __name__ == "__main__":
    main()
