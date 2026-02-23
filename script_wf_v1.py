from selenium import webdriver
import unittest, time, logging, warnings, os, sys, warnings, glob, pandas as pd, tqdm
import pathlib
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logging.basicConfig(
    filename="automatizacion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8",
)

sys.stdout.reconfigure(encoding="utf-8")
base_dir = pathlib.Path().resolve()
url_workforce = "http://amx-res-co.etadirect.com"
usuario_workforce = "38101491"
clave_workforce = "Diana2026++"
directorio_descargas = "C:/Users/P.A8EEC8CC92309B1/Downloads"
usuario_mysql = "ccot"
pass_mysql = "ccot"
host_mysql = "10.108.34.32"
puerto_mysql = "33063"
db_mysql = "ccot"
futuros_tabla = "wf_futuro_pruebas_stage"
chromedriver_path = (
    base_dir
    / "Script-BotCCOT Funcional"
    / "Script-BotCCOT"
    / "Scripts"
    / "drivers"
    / "chromedriver-114.0.5735.90.exe"
)
chrome_path = (
    base_dir
    / "Script-BotCCOT Funcional"
    / "Script-BotCCOT"
    / "Scripts"
    / "browser"
    / "chrome-win64-114.0.5735.90"
    / "chrome.exe"
)

if not chromedriver_path.exists():
    raise FileNotFoundError(f"No se encontró el ChromeDriver en: {chromedriver_path}")
else:
    logging.info(f"ChromeDriver encontrado en: {chromedriver_path}")

logging.info("Ruta completa del ChromeDriver:", chromedriver_path)


def delete_downloads_contents(downloads_folder):
    if os.path.exists(downloads_folder):
        for root, dirs, files in os.walk(downloads_folder):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                os.rmdir(dir_path)
        logging.info("Contenido de la carpeta Downloads eliminado correctamente.")
    else:
        logging.info("La carpeta Downloads no existe.")


def combinar_archivos_csv_descargas(ruta_descargas):
    import re

    warnings.filterwarnings("ignore", category=DeprecationWarning)
    archivos_csv = glob.glob(os.path.join(ruta_descargas, "*.csv"))
    todos_dataframes = []

    # Patrón regex para extraer lo que está entre Actividades- y _FECHA.csv
    patron = re.compile(r"Actividades-(.+?)_\d{2}_\d{2}_\d{2}\.csv")

    for archivo in tqdm.tqdm(archivos_csv, desc="Leyendo archivos a combinar"):
        df = pd.read_csv(archivo)
        nombre_archivo = os.path.basename(archivo)

        # Extrae el texto clave entre Actividades- y la fecha
        coincidencia = patron.search(nombre_archivo)

        if coincidencia:
            texto_clave = coincidencia.group(1)  # Extrae el texto entre los patrones
        else:
            texto_clave = "Origen_Desconocido"  # Valor por defecto si no coincide

        df["Origen"] = texto_clave  # Nueva columna con el texto clave
        todos_dataframes.append(df)

    logging.info("Uniendo archivos...")
    resultado_final = pd.concat(todos_dataframes, ignore_index=True)
    resultado_final["Fecha"] = pd.to_datetime(
        resultado_final["Fecha"], format="%d/%m/%y"
    )

    logging.info("Archivos unidos correctamente.")
    return resultado_final


def borrar_ultimos_archivos(ruta_descargas, num_archivos):
    archivos = sorted(
        glob.glob(os.path.join(ruta_descargas, "*")), key=os.path.getmtime, reverse=True
    )
    for archivo in archivos[: num_archivos - 1]:
        try:
            os.remove(archivo)
            logging.info(f"Archivo eliminado: {archivo}")
        except Exception as e:
            logging.info(f"No se pudo eliminar el archivo {archivo}: {e}")


warnings.filterwarnings("ignore")


# Si intentas entender este codigo, te recomiendo primero mirar el docs, y el arcivo de cierres.
class TestWorkforceAutomation(unittest.TestCase):
    def configuracion_workforce(self):
        vista = ActionChains(self.driver)
        vista.send_keys(Keys.TAB)
        vista.send_keys(Keys.SPACE)
        vista.perform()
        time.sleep(1)
        aplicar = WebDriverWait(self.driver, 50).until(
            EC.element_to_be_clickable((By.XPATH, "(//button[@title='Aplicar'])[2]"))
        )
        aplicar.click()
        time.sleep(1)
        boton_siguiente = WebDriverWait(self.driver, 12).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "span.app-button-icon.oj-ux-ico-chevron-right")
            )
        )
        #boton_siguiente.click()
        #boton_siguiente.click()

    def iniciar_sesion(self):
        max_logins = 3
        intentos_login = 0
        inicio_sesion_exitoso = False
        logging.info("Inicio proceso BotCCOT ...")
        while intentos_login < max_logins and not inicio_sesion_exitoso:
            try:
                usuario = WebDriverWait(self.driver, 50).until(
                    EC.element_to_be_clickable((By.ID, "username"))
                )
                contraseña = WebDriverWait(self.driver, 50).until(
                    EC.element_to_be_clickable((By.ID, "password"))
                )
                usuario.send_keys(usuario_workforce)
                contraseña.send_keys(clave_workforce)
                self.driver.find_element(By.XPATH, "//span[text()='Iniciar']").click()
                sesion_vieja = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "del-oldest-session"))
                )
                if sesion_vieja:
                    self.driver.find_element(By.ID, "del-oldest-session").click()
                    print("-----------------------------------------------------")
                    logging.info(
                        "Cerrando la sesión más antigua para poder continuar..."
                    )
                    contraseña = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "password"))
                    )
                    contraseña.send_keys(clave_workforce)
                    self.driver.find_element(
                        By.CSS_SELECTOR, "button#sign-in>div"
                    ).click()
                boton_vista = WebDriverWait(self.driver, 50).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "(//button[@title='Vista']//span)[2]")
                    )
                )
                if boton_vista:
                    boton_vista.click()
                    inicio_sesion_exitoso = True
                else:
                    inicio_sesion_exitoso = False
            except (NoSuchElementException, TimeoutException):
                intentos_login += 1
                logging.info(
                    f"Intento {intentos_login} fallido al intentar acceder a workforce"
                )
        return inicio_sesion_exitoso

    def realizar_descargas(self, dias, contador, botones_dict):
        # bucle de descarga-------------------------------------------------------------------
        max_intentos = 3
        for _ in range(dias):
            for nombre, detalles in botones_dict.items():
                operacion_exitosa = False
                intentos = 0
                while not operacion_exitosa and intentos < max_intentos:
                    try:
                        time.sleep(2)
                        # Realiza las operaciones de clic basadas en el diccionario
                        boton = WebDriverWait(self.driver, detalles["tiempo"]).until(
                            EC.element_to_be_clickable((By.XPATH, detalles["xpath"]))
                        )
                        boton.click()
                        boton_acciones = WebDriverWait(
                            self.driver, detalles["tiempo"]
                        ).until(
                            EC.element_to_be_clickable(
                                (By.XPATH, "//span[contains(.,'Acciones')]")
                            )
                        )
                        boton_acciones.click()
                        boton_exportar = WebDriverWait(
                            self.driver, detalles["tiempo"]
                        ).until(
                            EC.element_to_be_clickable(
                                (By.XPATH, "//span[contains(text(),'Exportar')]")
                            )
                        )
                        boton_exportar.click()

                        archivos_antes = os.listdir(directorio_descargas)
                        nuevo_archivo = None
                        extensiones_permitidas = (".csv",)
                        tiempo_max = 20
                        start_time = time.time()
                        while (
                            nuevo_archivo is None
                            or os.path.splitext(nuevo_archivo)[1].lower()
                            not in extensiones_permitidas
                        ):
                            if time.time() - start_time > tiempo_max:
                                break
                            archivos_actuales = os.listdir(directorio_descargas)
                            nuevos_archivos = [
                                f
                                for f in archivos_actuales
                                if f not in archivos_antes
                                and os.path.splitext(f)[1].lower()
                                in extensiones_permitidas + (".htm", ".html")
                            ]
                            if nuevos_archivos:
                                nuevo_archivo = os.path.join(
                                    directorio_descargas, nuevos_archivos[0]
                                )
                                extension = os.path.splitext(nuevo_archivo)[1]
                                if extension.lower() in (".htm", ".html"):
                                    boton_acciones.click()
                                    boton_exportar.click()
                            else:
                                time.sleep(4)  # CAMBIAR TIEMPO SI HAY LENTITUD EN OFSC
                        if nuevo_archivo:
                            logging.info(
                                f"Se ha descargado un nuevo archivo: \n{nuevo_archivo} \nArchivo número: {contador}"
                            )
                            contador += 1
                            operacion_exitosa = True
                        else:
                            logging.info(
                                "No se descargó un nuevo archivo en un tiempo limite(30s)"
                            )
                            logging.info("Reintentando...")
                            operacion_exitosa = False
                    except Exception as e:
                        intentos += 1
                        logging.info(f"Intento {intentos} fallido para {nombre}: {e}")

                    if operacion_exitosa:
                        logging.info(
                            f"Operación exitosa para {nombre} después de {intentos + 1} intentos."
                        )
                        print("-----------------------------------------------------")
                    elif intentos >= max_intentos:
                        logging.info(
                            f"No se pudo completar la operación para {nombre} después de {intentos} intentos, reintentando..."
                        )
                        break
            # Intenta avanzar al siguiente boton
            boton_siguiente = WebDriverWait(self.driver, 50).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "span.app-button-icon.oj-ux-ico-chevron-right")
                )
            )
            boton_siguiente.click()

        return contador

    def cargar_datos(self):
        # Combinar los archivos Excel en un solo dataframe
        datos_excel = combinar_archivos_csv_descargas(directorio_descargas)
        carga_exitosa = False
        while not carga_exitosa:
            try:
                # Crear el motor de conexión a la base de datos MySQL
                motor = create_engine(
                    f"mysql+pymysql://{usuario_mysql}:{pass_mysql}@{host_mysql}:{puerto_mysql}/{db_mysql}"
                )
                # Crear una sesión
                Session = sessionmaker(bind=motor)
                session = Session()
                # Contar las filas existentes en la tabla
                fila_contar = session.execute(
                    text(f"SELECT COUNT(*) FROM {futuros_tabla}")
                ).scalar()
                # Eliminar todas las filas de la tabla específica usando text
                session.execute(text(f"TRUNCATE TABLE {futuros_tabla}"))
                session.commit()
                logging.info(
                    f"Todas las filas eliminadas. Total de filas eliminadas: {fila_contar}"
                )
                # Cargar los datos de Excel a la tabla 'wf_futuro'
                datos_excel.to_sql(
                    futuros_tabla, con=motor, if_exists="append", index=False
                )
                logging.info("Datos subidos correctamente a la base de datos.")
                motor.dispose()
                carga_exitosa = True
                result_message_param = session.execute(
                    text("CALL actualizar_wf_om(@result_message)")
                )
                result_message_query = session.execute(text("SELECT @result_message"))
                result_message = result_message_query.fetchone()[0]
                session.commit()
                print(
                    "Procedimiento back O&M Fijo ejecutado correctamente, mensaje:",
                    result_message,
                )
            except Exception as e:
                logging.info(f"Ocurrió un error durante la carga de datos: {e}")
                logging.info("Volviendo a intentar...")
                try:
                    motor.dispose()
                except:
                    pass
                carga_exitosa = False
                time.sleep(10)

    def workforce_automatizacion(self):
        # diccionario Futuras-------------------------------------------------------------------------
        botones_futuras = {
            "REGION OCCIDENTE": {
                "xpath": "//span[text()='REGION OCCIDENTE'][1]",
                "tiempo": 50,
            },
            "PYMES OCCIDENTE": {
                "xpath": "//span[text()='PYMES OCCIDENTE'][1]",
                "tiempo": 50,
            },
            "DTH OCCIDENTE": {
                "xpath": "//span[text()='DTH OCCIDENTE'][1]",
                "tiempo": 50,
            },
        }
        # diccionario Hoy Mañana-------------------------------------------------------------------------
        botones_hoy_mañana = {
            "RECURSOS OCCIDENTE (INTEGRAL) SEG FIJA": {
                "xpath": "//span[text()='RECURSOS OCCIDENTE (INTEGRAL) SEG FIJA'][1]",
                "tiempo": 50,
            },
            "REGION OCCIDENTE": {
                "xpath": "//span[text()='REGION OCCIDENTE'][1]",
                "tiempo": 50,
            },
            "R3": {"xpath": "//span[text()='R3 -->'][1]", "tiempo": 50},
            "RECURSOS OCCIDENTE CLIMA Y FUERZA ALIAD": {
                "xpath": "//span[text()='RECURSOS OCCIDENTE  CLIMA Y FUERZA ALIAD'][1]",
                "tiempo": 50,
            },
            "PYMES OCCIDENTE": {
                "xpath": "//span[text()='PYMES OCCIDENTE'][1]",
                "tiempo": 50,
            },
            
        }
        selenium_exitoso = False

        while not selenium_exitoso:
            try:
                delete_downloads_contents(directorio_descargas)
                s = Service(executable_path=str(chromedriver_path))
                options = webdriver.ChromeOptions()
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_argument("--log-level=3")
                options.add_argument("--disable-logging")
                options.add_argument("--disable-notifications")
                options.add_argument("--disable-geolocation")
                options.add_argument("--disable-gpu")
                options.add_argument("--disable-software-rasterizer")
                options.add_argument("--no-sandbox")
                options.add_argument("window-size=1920,1080")
                #options.add_argument("--headless=new")
                options.add_argument(
                    "--blink-settings=imagesEnabled=false"
                )  # Esto deshabilita la carga de img.
                options.add_argument("--disable-plugins")
                options.add_argument("--disable-extensions")
                if chrome_path.exists():
                    options.binary_location = str(chrome_path)
                self.driver = webdriver.Chrome(service=s, options=options)
                self.driver.get(url_workforce)
                logging.getLogger("webdriver").setLevel(logging.ERROR)
                # Acá todo el inicio de sesion es en si un metodo, y se evalua con una variable.
                inicio_sesion_exitoso = self.iniciar_sesion()
                if not inicio_sesion_exitoso:
                    # Si el mensaje de abajo aparece muchas veces seguidas en teams, ojooo.
                    logging.info(
                        "workforce futuro falló en credenciales o página caída, proceso no completado exitosamente"
                    )
                    print("-----------------------------------------------------")
                    logging.info("Se alcanzó el número máximo de intentos fallidos.")
                    break

                print("-----------------------------------------------------")
                logging.info("Sesión iniciada correctamente en workforce")
                print("-----------------------------------------------------")
                # configuramos workforce a todos los datos de hijos y avanzamos 2 dias.
                self.configuracion_workforce()
                # Descargar a futuras (28 días, sin hoy ni mañana)
                # self.realizar_descargas(1, 1, botones_futuras)
                # Recargar la pagina para descargar hoy y mañana
                hoy_mañana_exitoso = False
                while not hoy_mañana_exitoso:
                    try:
                        self.driver.refresh()
                        contador = self.realizar_descargas(2, 1, botones_hoy_mañana)
                        self.cargar_datos()
                        logging.info("Proceso datas OFSC ejecutado correctamente.")
                        hoy_mañana_exitoso = True
                    except:
                        logging.info("Ha ocurrido un error, reintentando...")
                        hoy_mañana_exitoso = False
                        if contador > 0:
                            borrar_ultimos_archivos(directorio_descargas, contador)
                        contador = 0  # Reiniciar el contador después de borrar archivos fallidos

                        time.sleep(5)
                selenium_exitoso = True
            except Exception as e:
                logging.info(f"Ocurrió un error durante selenium: {e}")
                logging.info("Volviendo a intentar...")
                selenium_exitoso = False

    def test_workforce_automatizacion(self):
        self.workforce_automatizacion()

    def tearDown(self):
        self.driver.quit()


def ejecutar_automatizacion():
    try:
        suite = unittest.TestLoader().loadTestsFromTestCase(TestWorkforceAutomation)
        unittest.TextTestRunner().run(suite)
    except Exception as e:
        logging.info(f"⚠️ Error al ejecutar la automatización: {e}")
        logging.info(f"⚠️ Error inesperado en el bot Workforce: {e}")


if __name__ == "__main__":
    try:
        print("🚀 Ejecutando script de automatización Workforce...")
        ejecutar_automatizacion()
    except Exception as e:
        print(f"💥 Error crítico: {e}")
        logging.info(f"💥 Error crítico en el bot Workforce: {e}")
