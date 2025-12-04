import os
import logging
import re
import datasus_dbc
from dbfread import DBF
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(file_path)

scripts_dir = os.path.dirname(os.path.dirname(current_dir))
base_dir = os.path.dirname(scripts_dir)

dbc_dir = os.path.join(base_dir, "data", "utils", "dbc_datasus_sim")
parquet_dir = os.path.join(base_dir, "data", "raw")
os.makedirs(parquet_dir, exist_ok=True)

parquet_final_path = os.path.join(parquet_dir, "raw_sistema_info_mortalidade.parquet")

COLUNAS_FIM_ORDEM_FIXA = [
    "CONTADOR", "ORIGEM", "TIPOBITO", "DTOBITO", "HORAOBITO", "NATURAL", "CODMUNNATU", "DTNASC", "IDADE", "SEXO",
    "RACACOR", "ESTCIV", "ESC", "ESC2010", "SERIESCFAL", "OCUP", "CODMUNRES", "LOCOCOR", "CODESTAB", "ESTABDESCR",
    "CODMUNOCOR", "IDADEMAE", "ESCMAE", "ESCMAE2010", "SERIESCMAE", "OCUPMAE", "QTDFILVIVO", "QTDFILMORT", "GRAVIDEZ",
    "SEMAGESTAC", "GESTACAO", "PARTO", "OBITOPARTO", "PESO", "TPMORTEOCO", "OBITOGRAV", "OBITOPUERP", "ASSISTMED",
    "EXAME", "CIRURGIA", "NECROPSIA", "LINHAA", "LINHAB", "LINHAC", "LINHAD", "LINHAII", "CAUSABAS", "CB_PRE", "CRM",
    "COMUNSVOIM", "DTATESTADO", "CIRCOBITO", "ACIDTRAB", "FONTE", "NUMEROLOTE", "TPPOS", "DTINVESTIG", "CAUSABAS_O",
    "DTCADASTRO", "ATESTANTE", "STCODIFICA", "CODIFICADO", "VERSAOSIST", "VERSAOSCB", "FONTEINV", "DTRECEBIM",
    "ATESTADO", "DTRECORIGA", "CAUSAMAT", "ESCMAEAGR1", "ESCFALAGR1", "STDOEPIDEM", "STDONOVA", "DIFDATA",
    "NUDIASOBCO", "NUDIASOBIN", "DTCADINV", "TPOBITOCOR", "DTCONINV", "FONTES", "TPRESGINFO", "TPNIVELINV",
    "NUDIASINF", "DTCADINF", "MORTEPARTO", "DTCONCASO", "FONTESINF", "ALTCAUSA"
]
schema_pa = pa.schema([(name, pa.string()) for name in COLUNAS_FIM_ORDEM_FIXA])

def record_generator(dbf_file: DBF, columns: list, batch_size: int = 20000):
    """
    Lê registros do DBF em batches, garantindo ordenação, preenchimento de colunas faltantes e tipagem como string.
    Retorna uma Tabela PyArrow para cada batch, evitando o consumo excessivo de memória.
    """
    current_batch = []
    
    for record in dbf_file:
        ordered_record = {}
        for col in columns:
            val = record.get(col, '')
            ordered_record[col] = str(val).strip() 
        
        current_batch.append(ordered_record)
        
        if len(current_batch) >= batch_size:
            yield pa.Table.from_pylist(current_batch, schema=schema_pa)
            current_batch = []
            
    if current_batch:
        yield pa.Table.from_pylist(current_batch, schema=schema_pa)

ultimo_ano = None
if os.path.exists(parquet_final_path):
    logger.info(f"Arquivo Parquet existente encontrado: {parquet_final_path}. Verificando último ano na coluna DTOBITO...")
    
    try:
        df_parquet = pd.read_parquet(parquet_final_path, columns=["DTOBITO"], engine='pyarrow')
        
        df_parquet['ANO_OBITO'] = df_parquet['DTOBITO'].astype(str).str[-4:]
        
        df_parquet = df_parquet[df_parquet['ANO_OBITO'].str.isnumeric() & (df_parquet['ANO_OBITO'].str.len() == 4)]
        
        if not df_parquet.empty:
            ultimo_ano = df_parquet["ANO_OBITO"].astype(int).max()
            logger.info(f"Último ano encontrado no Parquet: {ultimo_ano}")
        else:
            logger.info("Coluna DTOBITO vazia ou inválida. Todos os arquivos serão processados.")
            
    except Exception as e:
        logger.warning(f"Erro ao ler ou processar DTOBITO do Parquet: {e}. Processando todos os arquivos como fallback.")
        pass
else:
    logger.info("Parquet não existe. Todos os arquivos serão processados.")

arquivos_dbc = [f for f in os.listdir(dbc_dir) if f.lower().endswith(".dbc")]

if ultimo_ano is not None:
    arquivos_filtrados = []
    for f in arquivos_dbc:
        match = re.search(r'(\d{4})', f)
        if match:
            ano_arquivo = int(match.group(1))
            if ano_arquivo > ultimo_ano: 
                arquivos_filtrados.append(f)
    arquivos_dbc = arquivos_filtrados

arquivos_dbc.sort()

if not arquivos_dbc:
    logger.info("Nenhum arquivo DBC novo para processar.")
    raise SystemExit(0)

logger.info(f"Arquivos a processar: {arquivos_dbc}")


parquet_exists = os.path.exists(parquet_final_path)

writer = None
if parquet_exists:
    logger.info("Inicializando ParquetWriter para anexação...")
    writer = pq.ParquetWriter(parquet_final_path, schema_pa) 
else:
    logger.info("Parquet não existe. Será criado na primeira iteração e o writer será inicializado em seguida.")


for arquivo_dbc in arquivos_dbc:
    caminho_completo_dbc = os.path.join(dbc_dir, arquivo_dbc)
    nome_dbf = arquivo_dbc.replace(".dbc", ".dbf")
    path_dbf = os.path.join(dbc_dir, nome_dbf)
    
    logger.info(f"--- Processando arquivo: {arquivo_dbc} ---")

    try:
        datasus_dbc.decompress(caminho_completo_dbc, dbf_path=path_dbf)
        logger.info(f"Arquivo descompactado para: {path_dbf}")

        dbf_data = DBF(path_dbf, encoding='latin1') 

        total_rows = 0
        
        generator = record_generator(dbf_data, COLUNAS_FIM_ORDEM_FIXA)
        
        for table_batch in generator:
            
            if writer is None:
                pq.write_table(table_batch, parquet_final_path)
                logger.info(f"Arquivo Parquet criado com o primeiro batch de {len(table_batch)} linhas.")
                writer = pq.ParquetWriter(parquet_final_path, schema_pa)
            else:
                writer.write_table(table_batch)
            
            total_rows += len(table_batch)
            logger.info(f"Batch de {len(table_batch)} linhas processado. Total até agora: {total_rows}")
        
        logger.info(f"Dados do arquivo {arquivo_dbc} processados e anexados ({total_rows} linhas no total).")
        
        #if os.path.exists(caminho_completo_dbc):
        #    os.remove(caminho_completo_dbc)
        #    logger.info(f"Arquivo DBC original removido: {caminho_completo_dbc}")    

    except Exception as e:
        logger.error(f"Erro CRÍTICO ao processar o arquivo {arquivo_dbc}. Pulando para o próximo. Erro: {e}")
        
    finally:
        if 'path_dbf' in locals() and os.path.exists(path_dbf):
             os.remove(path_dbf)
             logger.info(f"Arquivo DBF temporário removido: {path_dbf}")


if writer:
    writer.close()
    logger.info("Processamento de todos os arquivos DBC concluído.")