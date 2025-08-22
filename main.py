from flask import Flask, Response
import pandas as pd
import datetime
from io import BytesIO
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext

app = Flask(__name__)

def obtener_datos_sharepoint():
    site_url = "https://depilzone001-my.sharepoint.com/personal/dataanalytics_depilzone001_onmicrosoft_com"
    file_url = "/personal/dataanalytics_depilzone001_onmicrosoft_com/Documents/REGISTRO DE VENTAS 2025.xlsx"
    usuario = "DataAnalytics@DEPILZONE001.onmicrosoft.com"
    password = "Luis4007"

    ctx_auth = AuthenticationContext(site_url)
    if ctx_auth.acquire_token_for_user(usuario, password):
        ctx = ClientContext(site_url, ctx_auth)
        buffer = BytesIO()
        ctx.web.get_file_by_server_relative_url(file_url).download(buffer).execute_query()
        buffer.seek(0)
        df = pd.read_excel(buffer, sheet_name="DATA")
        return df
    else:
        raise Exception("Error en la autenticación SharePoint")

def procesar_dataframes():
    dfc = obtener_datos_sharepoint().copy()
    pattern_inicio_final = r'^[\s\u00A0\u2000-\u200B\u202F\u205F\u200C\u200D\uFEFF\x00-\x1F\x7F]+|[\s\u00A0\u2000-\u200B\u202F\u205F\u200C\u200D\uFEFF\x00-\x1F\x7F]+$'
    dfc['IDCITA_CLEAR'] = dfc['IDCITA'].astype(str).fillna('').str.replace(pattern_inicio_final, '', regex=True)
    dfc['IDCITA_CLEAR_2'] = dfc['IDCITA_CLEAR'].astype(str).fillna('').str.replace(r'^[^0-9]+|[^0-9]+$', '', regex=True)
    dfc['IDCITA_CLEAR_3'] = dfc['IDCITA_CLEAR_2'].astype(str).str.findall(r"\d{5,}")
    dfc['IDCITA_CLEAR_4'] = dfc['IDCITA_CLEAR_3']
    dfc_final = dfc.explode('IDCITA_CLEAR_4')


    dfc_final[['IDCITA_CLEAR_2', 'IDCITA_CLEAR_4']] =  dfc_final[['IDCITA_CLEAR_2', 'IDCITA_CLEAR_4']].replace('', pd.NA)
    dfc_final['IDCITA_CLEAR_5'] =  dfc_final['IDCITA_CLEAR_4'].combine_first( dfc_final['IDCITA_CLEAR_2'])

    idcitas_agendados = dfc_final[['IDCITA','IDCITA_CLEAR','IDCITA_CLEAR_2','IDCITA_CLEAR_3','IDCITA_CLEAR_4']].copy()
    idcitas_agendados[['IDCITA_CLEAR_2', 'IDCITA_CLEAR_4']] = idcitas_agendados[['IDCITA_CLEAR_2', 'IDCITA_CLEAR_4']].replace('', pd.NA)
    idcitas_agendados['IDCITA_CLEAR_5'] = idcitas_agendados['IDCITA_CLEAR_4'].combine_first(idcitas_agendados['IDCITA_CLEAR_2'])

    valores_unicos = idcitas_agendados['IDCITA_CLEAR_5'].unique()
    DF_IDCITAS_UNICOS = pd.DataFrame(valores_unicos, columns=['id_citas'])

    return dfc_final, idcitas_agendados, DF_IDCITAS_UNICOS

@app.route("/dfc_final")
def endpoint_dfc_final():
    dfc_final, _, _ = procesar_dataframes()
    return Response(dfc_final.to_csv(index=False), mimetype="text/csv")



@app.route("/DF_IDCITAS_UNICOS")
def endpoint_df_idcitas_unicos():
    _, _, DF_IDCITAS_UNICOS = procesar_dataframes()
    return Response(DF_IDCITAS_UNICOS.to_csv(index=False), mimetype="text/csv")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)


