#Entry point: cf_IngestTrigger

import requests

def cf_IngestTrigger(request):
  prj = "dbar-pers"

  tables = ["customers", "employees", "offices", "orders", "payments", "productlines", "products"]

  for t in tables:
    # definimos los parametros de entrada al endpoint
    PARAMS = {'prj':prj, 'table': t}
    URL = f"https://us-central1-{prj}.cloudfunctions.net/cf_IngestTable"

    # enviamos get request y grabamos la respuesta
    r = requests.get(url = URL, params = PARAMS)
    print(f"Enviando trigger para tabla: {t}")

  return "cf_IngestTrigger fue activado exitosamente!"

# requirements.txt
# # Function dependencies, for example:
# # package>=version
# requests
# google-api-python-client
# google-cloud-bigquery
# google-cloud-bigquery-storage
# pandas