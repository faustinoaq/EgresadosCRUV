from flask import Flask, render_template, request, redirect, url_for, jsonify
from sqlalchemy import create_engine
from PyPDF2 import PdfReader
from pathlib import Path
import threading
import tabula
import glob
import pdb
import os
import re

app = Flask(__name__)
archivos = {}
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:Passw0rd@100.67.105.49:33306/egresados'
engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=True)

PATH = '/home/user/Projects/Egresados'

def to_sql(df, create=False):
   if create:
      df.to_sql('Egresados', engine, if_exists='append')
      #df.to_sql('graduados', engine, if_exists='append')
      #df.to_sql('titulos', engine, if_exists='append')
      try:
         with engine.begin() as conn:
            conn.execute('ALTER TABLE Egresados ADD PRIMARY KEY (`Num. Diploma`);')
            conn.execute('ALTER TABLE graduados ADD PRIMARY KEY (`ID_EGRESADO`);')
            conn.execute('ALTER TABLE titulo ADD PRIMARY KEY (`ID_EGRESADO`);')
      except:
         pass
   else:
      df.to_sql('EgresadosTemp', engine, if_exists='replace')
      #df.to_sql('graduadosTemp', engine, if_exists='replace')
      #df.to_sql('titulosTemp', engine, if_exists='replace')
      with engine.begin() as conn:
         conn.execute('REPLACE INTO Egresados (SELECT * FROM EgresadosTemp)')
         #conn.execute('REPLACE INTO graduados (SELECT * FROM graduadosTemp)')
         #conn.execute('REPLACE INTO titulos (SELECT * FROM titulosTemp)')

def process_file(filename):
   input_file = f'{PATH}/pdf/{filename}'
   total = 0
   reader = PdfReader(input_file)
   for i in range(len(reader.pages)):
      dfs = tabula.io.read_pdf(input_file,guess=False, columns=[210, 340, 759, 850, 940], pages=str(i+1))
      #dfs = tabula.io.read_pdf(input_file,guess=False, columns=[210, 340, 759, 850, 940], pages=[1])
      for j, df in enumerate(dfs):
         try:
            graduados = pd.DataFrame()
            graduados.columns = [] 
            df = df.loc[:,(~df.isnull()).any()]
            df = df[(~df.isnull()).all(axis=1)]
            df.columns = ['Nombre', 'Cedula', 'Titulo', 'Fecha Diploma', 'Num. Diploma', 'Indice']
            df = df[(~df['Num. Diploma'].str.startswith('no'))]
            df = df[(df.Nombre != 'Nombre')]
            df = df['Cedula'].str.split().join('-')
            df['Año'] = df['Fecha Diploma'].str.extract(r'.*-.*-(\d+)').astype(int) + 2000
            df['Num. Diploma'] = df['Num. Diploma'].str.extract(r'(\d+)').astype(int)
            df['Correo'] = ""
            df['Telefono'] = ""
            df = df.set_index('Num. Diploma')
            # New tables
            if j == 0 and i == 0:
               to_sql(df[0:0], create=True) # Make sure Table exists
            to_sql(df)
            total += len(df.index)
            #print(df.to_string())
         except Exception as e:
            print(e)
            #pdb.set_trace()
            pass
   os.remove(input_file)
   print(f'Total {total} egresados')

@app.route('/data')
def data():
   draw = request.args.get('draw')
   start = request.args.get('start')
   length = request.args.get('length')
   search = request.args.get('search[value]')
   egresados = {
      'draw': int(draw),
      'recordsTotal': 0,
      'recordsFiltered': 0,
      'data': []
   }
   searches = search.split(';')
   query = f''
   searches = [s for s in searches if s] # Clean empty search
   if len(searches) == 1:
      search = searches[0]
      query = f'WHERE Nombre = \'{search}\' or Cedula = \'{search}\' OR Titulo = \'{search}\' OR `Fecha Diploma` = \'{search}\' OR Indice = \'{search}\' OR `Año` = \'{search}\''
   elif len(searches) > 1:
      query = 'WHERE '
      search = searches[0]
      query += f'(Nombre = \'{search}\' or Cedula = \'{search}\' OR Titulo = \'{search}\' OR `Fecha Diploma` = \'{search}\' OR Indice = \'{search}\' OR `Año` = \'{search}\')'
      searches = searches[1:]
      for search in searches:
         query += f' AND (Nombre = \'{search}\' or Cedula = \'{search}\' OR Titulo = \'{search}\' OR `Fecha Diploma` = \'{search}\' OR Indice = \'{search}\' OR `Año` = \'{search}\')'
   try:
      with engine.begin() as conn:
         total = conn.execute(f'SELECT COUNT(Nombre) FROM Egresados {query}').scalar()
         egresados['recordsFiltered'] = egresados['recordsTotal'] = int(total)
         results = conn.execute(f'SELECT * FROM Egresados {query} LIMIT {int(start)}, {int(length)}')
         for result in results:
            egresados['data'].append(list(dict(result).values()))
   except:
      pass
   return jsonify(egresados)

@app.route('/')
def index():
   for filename in list(archivos):
      if not archivos[filename].is_alive():
         del archivos[filename]
   return render_template('./index.html', archivos=archivos)

@app.route('/', methods = ['POST']) 
def upload_file():
   f = request.files['file']
   pdf = Path(f'{PATH}/pdf/{f.filename}')
   if not pdf.exists():
      f.save(f'{PATH}/pdf/{f.filename}')
      process_file_thread = threading.Thread(target=process_file, name='Parser', args=[f.filename])
      process_file_thread.start()
      archivos[f.filename] = process_file_thread
   return redirect(url_for('index'))

def cleanup():
   pdfs = glob.glob(f'{PATH}/pdf/*')
   for pdf in pdfs:
      os.remove(pdf)

if __name__ == '__main__':
   cleanup()
   app.run(host='0.0.0.0', port=5000, debug = False)
