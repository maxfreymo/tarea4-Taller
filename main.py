import requests
import xml.etree.ElementTree as ET
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

# indicadores que me interesa guardar
valid_indicators = [
    # death_indicators
    "Number of deaths",
    "Number of infant deaths",
    "Number of under-five deaths",
    "Mortality rate for 5-14 year-olds (probability of dying per 1000 children aged 5-14 years)",
    "Adult mortality rate (probability of dying between 15 and 60 years per 1000 population)",
    "Estimates of number of homicides",
    "Estimates of rates of homicides per 100 000 population",
    "Crude suicide rates (per 100 000 population)",
    "Mortality rate attributed to unintentional poisoning (per 100 000 population)",
    "Number of deaths attributed to non-communicable diseases, by type of disease and sex",
    "Estimated road traffic death rate (per 100 000 population)",
    "Estimated number of road traffic deaths",
    # weight_indicators
    "Mean BMI (kg/m&#xb2;) (crude estimate)",
    "Mean BMI (kg/m&#xb2;) (age-standardized estimate)",
    "Prevalence of obesity among adults, BMI &GreaterEqual; 30 (crude estimate) (%)",
    "Prevalence of obesity among children and adolescents, BMI > +2 standard deviations above the median (crude estimate) (%)",
    "Prevalence of overweight among adults, BMI &GreaterEqual; 25 (age-standardized estimate) (%)",
    "Prevalence of overweight among children and adolescents, BMI > +1 standard deviations above the median (crude estimate) (%)",
    "Prevalence of underweight among adults, BMI < 18.5 (age-standardized estimate) (%)",
    "Prevalence of thinness among children and adolescents, BMI < -2 standard deviations below the median (crude estimate) (%)",

    # other_indicators
    "Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol)",
    "Estimate of daily cigarette smoking prevalence (%)",
    "Estimate of daily tobacco smoking prevalence (%)",
    "Estimate of current cigarette smoking prevalence (%)",
    "Estimate of current tobacco smoking prevalence (%)",
    "Mean systolic blood pressure (crude estimate)",
    "Mean fasting blood glucose (mmol/l) (crude estimate)",
    "Mean Total Cholesterol (crude estimate)"
]

# Defino la estructura de mi dataframe como un diccionario
df_dict = {'GHO': list(),
           'COUNTRY': [],
           'SEX': [],
           'YEAR': [],
           'GHECAUSES': [],
           'AGEGROUP': [],
           'Display': [],
           'Numeric': [],
           'Low': [],
           'High': []
           }


# Acceso Google Sheet
gc = gspread.service_account(
    filename='taller-tarea-4-316416-de9f63bf1491.json')
sh = gc.open_by_key('1d9SmHBfk6aDSYA8uPClsW5fxSdGfNFajQk-zcuujyb8')
worksheet = sh.get_worksheet(0)

# Función para construir el dataframe


def build_df(pais, data_frame):
    url = 'http://tarea-4.2021-1.tallerdeintegracion.cl/gho_{}.xml'.format(
        pais)
    response = requests.get(url)

    if response.status_code == 200:
        # Guardo el árbol
        print('¡Contacto con la API de {} exitosa!'.format(pais))
        tree = ET.fromstring(response.content)

        # El árbol tiene nodos Fact
        nodos_fact = tree.findall('Fact')
        contador = 0
        for child in nodos_fact:
            try:
                indicator = child.find('GHO').text

            except Exception as e:
                print('La excepción es {}'.format(e))
                indicator = None

            # Si el indicador se encuentra en la lista de indicadores válidos, entonces guardo elementos del nodo
            if indicator in valid_indicators:
                data_frame['GHO'].append(indicator)

                try:
                    country = child.find('COUNTRY').text
                except:
                    country = None
                data_frame['COUNTRY'].append(country)

                try:
                    sex = child.find('SEX').text
                except:
                    sex = None
                data_frame['SEX'].append(sex)

                try:
                    year = child.find('YEAR').text
                except:
                    year = None
                data_frame['YEAR'].append(year)

                try:
                    ghecauses = child.find('GHECAUSES').text
                except:
                    ghecauses = None
                data_frame['GHECAUSES'].append(ghecauses)

                try:
                    agegroup = child.find('AGEGROUP').text
                except:
                    agegroup = None
                data_frame['AGEGROUP'].append(agegroup)

                try:
                    display = child.find('Display').text
                except:
                    display = -1
                data_frame['Display'].append(display)

                try:
                    numeric = float(child.find('Numeric').text)
                except:
                    numeric = -1
                data_frame['Numeric'].append(numeric)

                try:
                    low = float(child.find('Low').text)
                except:
                    low = -1
                data_frame['Low'].append(low)

                try:
                    high = float(child.find('High').text)
                except:
                    high = -1
                data_frame['High'].append(high)

                contador = contador + 1

        print(contador)
        return (data_frame)


# Añado la información de Chile al dataframe
df_dict = build_df('CHL', df_dict)
# Añado la información de Rumania al dataframe
df_dict = build_df('ROU', df_dict)
# Añado la información de Rumania al dataframe
df_dict = build_df('KAZ', df_dict)
# Añado la información de Rumania al dataframe
df_dict = build_df('MWI', df_dict)
# Añado la información de Rumania al dataframe
df_dict = build_df('AUS', df_dict)
# Añado la información de Rumania al dataframe
df_dict = build_df('ECU', df_dict)

# Dataframe final
df = pd.DataFrame(data=df_dict)

# Guardo el dataframe en spread sheet
set_with_dataframe(worksheet, df)
