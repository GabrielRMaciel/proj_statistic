# app.py
import os
import sqlite3
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from werkzeug.utils import secure_filename
import matplotlib.pyplot as plt
import matplotlib
import click
from datetime import datetime

# Configurações do Matplotlib para rodar em servidor e ter um estilo bonito
matplotlib.use('Agg')
plt.style.use('seaborn-v0_8-whitegrid')

# --- Configuração Inicial da Aplicação ---
UPLOAD_FOLDER = 'uploads'
DATABASE = 'analise_combustiveis.db'
ALLOWED_EXTENSIONS = {'csv'}
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.secret_key = 'beta-version-secret-key-final'

# --- Funções do Banco de Dados e Comandos CLI ---
def get_db_connection():
    conn = sqlite3.connect(DATABASE)
    return conn

def init_db():
    conn = get_db_connection()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS precos_combustiveis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            regiao_sigla TEXT, estado_sigla TEXT, municipio TEXT,
            revenda TEXT, cnpj_revenda TEXT, produto TEXT,
            data_coleta DATE, valor_venda REAL, valor_compra REAL,
            unidade_medida TEXT, bandeira TEXT,
            UNIQUE(cnpj_revenda, data_coleta, produto)
        )
    ''')
    conn.commit()
    conn.close()

@app.cli.command('init-db')
def init_db_command():
    """Limpa e cria o banco de dados."""
    init_db()
    click.echo('Banco de dados de combustíveis inicializado com sucesso.')

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# --- Rota Principal: O Dashboard Analítico ---
@app.route('/')
def dashboard():
    conn = get_db_connection()
    try:
        df = pd.read_sql_query("SELECT * FROM precos_combustiveis", conn)
        df['data_coleta'] = pd.to_datetime(df['data_coleta'])
    except (pd.io.sql.DatabaseError, ValueError):
        df = pd.DataFrame()
    finally:
        conn.close()

    if df.empty:
        flash("Bem-vindo ao Fuel-Analyzer! Sua base de dados está vazia. Comece enviando um arquivo.", "info")
        return render_template('dashboard.html', analyses={})

    analyses = {}

    # 1. KPIs
    analyses['kpi_total_registros'] = len(df)
    analyses['kpi_data_inicio'] = df['data_coleta'].min().strftime('%d/%m/%Y')
    analyses['kpi_data_fim'] = df['data_coleta'].max().strftime('%d/%m/%Y')
    analyses['kpi_preco_medio_geral'] = df['valor_venda'].mean()

    # 2. Série Temporal
    df_serie_temporal = df.groupby(['data_coleta', 'produto'])['valor_venda'].mean().unstack()
    principais_produtos = df['produto'].value_counts().nlargest(4).index
    plt.figure(figsize=(12, 6))
    for produto in principais_produtos:
        if produto in df_serie_temporal.columns:
            df_serie_temporal[produto].rolling(window=30, min_periods=1).mean().plot(label=produto)
    plt.title('Evolução do Preço Médio dos Combustíveis (Média Móvel 30 dias)')
    plt.ylabel('Preço Médio (R$)')
    plt.xlabel('Data da Coleta')
    plt.legend()
    plt.tight_layout()
    analyses['plot_serie_temporal'] = 'static/images/plot_serie_temporal.png'
    plt.savefig(analyses['plot_serie_temporal'])
    plt.close()

    # 3. Análise Geográfica
    preco_medio_estado = df.groupby('estado_sigla')['valor_venda'].mean().sort_values()
    analyses['top_5_estados_baratos'] = preco_medio_estado.head(5).to_dict()
    analyses['top_5_estados_caros'] = preco_medio_estado.tail(5).sort_values(ascending=False).to_dict()

    # 4. Distribuição de Preços
    produto_mais_comum = df['produto'].mode()[0]
    precos_produto_comum = df[df['produto'] == produto_mais_comum]['valor_venda']
    plt.figure(figsize=(10, 5))
    plt.hist(precos_produto_comum, bins=30, color='skyblue', edgecolor='black')
    plt.title(f'Distribuição de Preços para {produto_mais_comum}')
    plt.xlabel('Valor de Venda (R$)')
    plt.ylabel('Frequência')
    plt.tight_layout()
    analyses['plot_distribuicao_precos'] = 'static/images/plot_distribuicao_precos.png'
    analyses['produto_analisado'] = produto_mais_comum
    plt.savefig(analyses['plot_distribuicao_precos'])
    plt.close()
    
    return render_template('dashboard.html', analyses=analyses, now=datetime.utcnow().timestamp())

# --- Rota de Upload com Barra de Progresso ---
@app.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            return jsonify(success=False, message='Nenhum arquivo selecionado.')
        file = request.files['file']
        if file.filename == '':
            return jsonify(success=False, message='Nenhum arquivo selecionado.')
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            try:
                conn = get_db_connection()
                existing_records = pd.read_sql_query("SELECT cnpj_revenda, data_coleta, produto FROM precos_combustiveis", conn)
                df_new = pd.read_csv(file_path, delimiter=';', decimal=',')
                df_new.dropna(how='all', inplace=True)
                df_new = df_new.rename(columns={'Regiao - Sigla': 'regiao_sigla', 'Estado - Sigla': 'estado_sigla', 'Municipio': 'municipio', 'Revenda': 'revenda', 'CNPJ da Revenda': 'cnpj_revenda', 'Produto': 'produto', 'Data da Coleta': 'data_coleta', 'Valor de Venda': 'valor_venda', 'Valor de Compra': 'valor_compra', 'Unidade de Medida': 'unidade_medida', 'Bandeira': 'bandeira'})
                df_new.dropna(subset=['cnpj_revenda', 'data_coleta', 'produto'], inplace=True)
                df_new.drop_duplicates(subset=['cnpj_revenda', 'data_coleta', 'produto'], inplace=True)
                for col in ['cnpj_revenda', 'produto']:
                    df_new[col] = df_new[col].astype(str).str.strip()
                    if not existing_records.empty:
                        existing_records[col] = existing_records[col].astype(str).str.strip()
                df_new['data_coleta'] = pd.to_datetime(df_new['data_coleta'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
                if not existing_records.empty:
                    existing_records['data_coleta'] = pd.to_datetime(existing_records['data_coleta'], errors='coerce').dt.strftime('%Y-%m-%d')
                df_merged = df_new.merge(existing_records, on=['cnpj_revenda', 'data_coleta', 'produto'], how='left', indicator=True)
                df_to_insert = df_merged[df_merged['_merge'] == 'left_only']
                if not df_to_insert.empty:
                    colunas_db = ['regiao_sigla', 'estado_sigla', 'municipio', 'revenda', 'cnpj_revenda', 'produto', 'data_coleta', 'valor_venda', 'valor_compra', 'unidade_medida', 'bandeira']
                    df_to_insert[colunas_db].to_sql('precos_combustiveis', conn, if_exists='append', index=False)
                conn.close()
                novos = len(df_to_insert)
                duplicados = len(df_new) - novos
                
                flash(f'Arquivo processado! {novos} novos registros foram adicionados e {duplicados} duplicados foram ignorados.', 'success')
                return jsonify(success=True, redirect_url=url_for('dashboard'))

            except Exception as e:
                return jsonify(success=False, message=f'Ocorreu um erro ao processar o arquivo: {e}')
        else:
            return jsonify(success=False, message='Tipo de arquivo não permitido.')
    
    return render_template('upload.html')

if __name__ == '__main__':
    app.run(debug=True)