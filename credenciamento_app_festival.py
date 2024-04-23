import numpy as np 
import pandas as pd 
import streamlit as st 
import hashlib
import xlsxwriter
import io

# Application Related Module
from gspread_pandas import Spread,Client
from google.oauth2 import service_account
from gsheetsdb import connect
from streamlit_autorefresh import st_autorefresh
from datetime import datetime,timedelta

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive',
         "https://www.googleapis.com/auth/spreadsheets"]

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=scope
)

conn = connect(credentials=credentials)
client = Client(scope=scope,creds=credentials)
data_base_name = "dados_dashboardapp_dev"
data_base = Spread(data_base_name,client = client)
sh = client.open(data_base_name)
worksheet_list = sh.worksheets()

# ------------- PARAMETERS, VARIABLES & SETUPS ------------- 

## PAGE SETUP
st.set_page_config(layout="wide")
st_autorefresh(interval=5 * 60 * 1000, key="dataframerefresh")

## SIDEBAR FILTERS

input_filter = ['Credenciamento','Consulta Credenciados']
input_producer = 'Funn'
input_event = 'Bothanic 2023'

## SPREADSHEET MAPPING
spreadsheet_credenciamento = 'apoio_credenciamento'
spreadsheet_dei = 'de&i'

spreadsheet_producer = 'cadastro_produtora'
columns_producer = ['timestamp','no_producer','ds_address']

spreadsheet_events = 'cadastro_evento'
columns_events = ['timestamp','no_producer','no_event','ds_local']

spreadsheet_supplier = 'cadastro_fornecedor'
columns_supplier = ['timestamp','no_producer','no_event','tp_operation','no_supplier']

spreadsheet_operation = 'cadastro_operacao'
columns_operation = ['timestamp','tp_operation']

## DATAFRAME STRUCTURE
columns_dei = ['timestamp','no_producer','no_event','tp_operation','id_hash_person','nu_age','ds_gender','st_trans','ds_race','st_pcd']
columns_credenciamento = ['timestamp','no_producer','no_event','tp_operation','no_responsible','ds_tel_contact','ds_email_contact','no_credenciado', 'nu_cpf','ds_function','st_checkin','st_checkout','nu_pulseira','time_checkin','time_checkout']
colunmns_rename = {'timestamp':'Horário de Credenciamento','no_producer':'Produtora','no_event':'Evento','no_responsible':'Nome do responsável','ds_tel_contact':'Contato do responsável','ds_email_contact':'Email do responsável','no_credenciado':'Nome do credenciado', 'nu_cpf': 'CPF','ds_function':'Função','tp_operation':'Empresa','st_checkin': 'Fez check-in?','st_checkout':'Fez check-out?','nu_pulseira':'Pulseira registrada','time_checkin':'Horário de check-in','time_checkout':'Horário de check-out'}
## SELECTBOX OPTIONS

tp_gender = ['Homem','Mulher','Não binário','Prefiro não declarar']
tp_race = ['Branco','Pardo','Preto', 'Prefiro não declarar']
tp_binary_dei = ['Sim', 'Não', 'Prefiro não declarar']

# ------------- FUNCTIONS ------------- 

def send_form (opt,spreadsheet_choice,columns):
    df_opt = pd.DataFrame(opt)
    worksheet = sh.worksheet(spreadsheet_choice)
    
    df = pd.DataFrame(worksheet.get_all_records())
    result_df = pd.concat([df, df_opt], ignore_index=False)

    data_base.df_to_sheet(result_df[columns],sheet = spreadsheet_choice,index = False)

def update_data (opt,spreadsheet_choice,columns):
    df_opt = opt
    data_base.df_to_sheet(df_opt[columns],sheet = spreadsheet_choice,index = False)

def validacao_cpf(CPF):
    CPF = CPF.replace('.','').replace('-','')
    if len(CPF) == 11:
        
        primeiro1 = int(CPF[0]) * 10
        primeiro2 = int(CPF[1]) * 9
        primeiro3 = int(CPF[2]) * 8
        primeiro4 = int(CPF[3]) * 7
        primeiro5 = int(CPF[4]) * 6
        primeiro6 = int(CPF[5]) * 5
        primeiro7 = int(CPF[6]) * 4
        primeiro8 = int(CPF[7]) * 3
        primeiro9 = int(CPF[8]) * 2
        soma_validacao = (primeiro1 + primeiro2 + primeiro3 + primeiro4 + primeiro5 + primeiro6 + primeiro7 + primeiro8 + primeiro9)
        resto = soma_validacao%11

        if resto == 0 or resto ==1:
            dig_dezena = 0
        else:
            dig_dezena = 11-resto
        
        primeiro1 = int(CPF[0]) * 11
        primeiro2 = int(CPF[1]) * 10
        primeiro3 = int(CPF[2]) * 9
        primeiro4 = int(CPF[3]) * 8
        primeiro5 = int(CPF[4]) * 7
        primeiro6 = int(CPF[5]) * 6
        primeiro7 = int(CPF[6]) * 5
        primeiro8 = int(CPF[7]) * 4
        primeiro9 = int(CPF[8]) * 3
        primeiro10 = dig_dezena * 2
        
        soma_validacao = (primeiro1 + primeiro2 + primeiro3 + primeiro4 + primeiro5 + primeiro6 + primeiro7 + primeiro8 + primeiro9 + primeiro10)

        resto = soma_validacao%11
        if resto == 0 or resto ==1:
            dig_unidade = 0
        else:
            dig_unidade = 11-resto

        if (int(CPF[9])!=dig_dezena) or (int(CPF[10])!=dig_unidade):
            return False
        else: 
            return True
def check_cpf_event_duplicity (cpf:str,edition:str,dataframe:pd.DataFrame):
    if dataframe.empty:
        return False
    else:
        check_event = edition
        check_cpf = cpf
        check_cpf = input_cpf.replace('.','').replace('-','')
        check_cpf = str(check_cpf[0:3])+'.'+str(check_cpf[3:6])+'.'+str(check_cpf[6:9])+'-'+str(check_cpf[-2:])  
        df_check_cpf = dataframe[dataframe[['nu_cpf', 'no_event']].isin([check_cpf, check_event]).all(axis=1)]
        if(df_check_cpf.empty):
            return False
        else:
            return True
# ------------------- CREATE DATAFRAMES & SIDEBARFILTERS ---------------------
st.sidebar.header(input_producer+' - Credenciamento')
st.sidebar.subheader(input_event)
st.sidebar.subheader(" ")

df_operation = pd.DataFrame(sh.worksheet(spreadsheet_operation).get_all_records())
tp_operation = df_operation['tp_operation'].sort_values(ascending=True)

df_producers = pd.DataFrame(sh.worksheet(spreadsheet_producer).get_all_records())
no_producers = df_producers['no_producer'].sort_values(ascending=True)

no_events = input_event

input_edição = st.sidebar.selectbox('Edição',['29/09 - Péricles','07/10 - Sarau do Eva','21/10 - Aniverário VB','25/11 - A faculdade - Jeito Moleque e Inimigos da HP','02/12 - Fica Comigo','22/12 - Belo'])

df_supplier = pd.DataFrame(sh.worksheet(spreadsheet_supplier).get_all_records())
df_supplier = df_supplier[(df_supplier['no_event']==input_event) & (df_supplier['no_producer']==input_producer)]
no_supplier = df_supplier[['no_supplier','no_responsible','ds_tel_contact','ds_email_contact']]

input_choice = st.sidebar.selectbox('Formulário',input_filter)
st.sidebar.markdown("**[Acesso ao Dashboard Funn](%s)**" % 'https://venturo-funn-sustainability.streamlit.app')

# ------------------- BODY ---------------------
spacer1, header, spacer2 = st.columns((.1, 4, .1))
with header:
    st.title(f"{input_choice}")

if input_choice == 'Consulta Credenciados':

    spreadsheet_choice = spreadsheet_credenciamento
    columns = columns_credenciamento

    df_credenciados = pd.DataFrame(sh.worksheet(spreadsheet_choice).get_all_records())
    df_credenciados_original = pd.DataFrame(sh.worksheet(spreadsheet_choice).get_all_records())
    if not df_credenciados.empty:
        no_empresas = df_credenciados['tp_operation'].sort_values(ascending=True)
        no_empresas.loc['tp_operation'] = 'Todas as empresas'
        no_empresas = np.unique(no_empresas)
        spacer1, body, spacer2 = st.columns((.1, 4, .1))
        with body:

            input_empresa = st.selectbox("Empresa", no_empresas)
            input_export = st.selectbox("Tipo de relatório", ['Todos os colaboradores','Credenciados','Não credenciados'])

            if input_empresa=='Todas as empresas':
                if input_export == 'Credenciados':
                    df_credenciados = df_credenciados[(df_credenciados['no_event']==input_edição) & ((df_credenciados['st_checkin']==True) | (df_credenciados['st_checkin']=='TRUE'))]
                    df_export = df_credenciados[(df_credenciados['no_event']==input_edição) & ((df_credenciados['st_checkin']==True) | (df_credenciados['st_checkin']=='TRUE'))]
                elif input_export == 'Não credenciados':
                    df_credenciados = df_credenciados[(df_credenciados['no_event']==input_edição)  & ((df_credenciados['st_checkin']==False) | (df_credenciados['st_checkin']=='FALSE'))]
                    df_export = df_credenciados[(df_credenciados['no_event']==input_edição)  & ((df_credenciados['st_checkin']==False) | (df_credenciados['st_checkin']=='FALSE'))]
                else:
                    df_credenciados = df_credenciados[df_credenciados['no_event']==input_edição]
                    df_export = df_credenciados[(df_credenciados['no_event']==input_edição)]

                df_credenciados = df_credenciados[['tp_operation','no_credenciado','nu_cpf','ds_function','nu_pulseira','st_checkin','st_checkout']]
                df_credenciados = df_credenciados.rename(columns={'tp_operation': 'Empresa','no_credenciado':'Nome do colaborador', 'nu_cpf': 'CPF','ds_function':'Função','nu_pulseira':'Nro. da pulseira'})
                df_credenciados['Nro. da pulseira'] = df_credenciados['Nro. da pulseira'].astype('string')
            
            else:
                if input_export == 'Credenciados':
                    df_credenciados = df_credenciados[(df_credenciados['tp_operation']==input_empresa) & (df_credenciados['no_event']==input_edição) & ((df_credenciados['st_checkin']==True) | (df_credenciados['st_checkin']=='TRUE'))]
                    df_export = df_credenciados[(df_credenciados['tp_operation']==input_empresa) & (df_credenciados['no_event']==input_edição) & ((df_credenciados['st_checkin']==True) | (df_credenciados['st_checkin']=='TRUE'))]
                elif input_export == 'Não credenciados':
                    df_credenciados = df_credenciados[(df_credenciados['tp_operation']==input_empresa) & (df_credenciados['no_event']==input_edição) & ((df_credenciados['st_checkin']==False) | (df_credenciados['st_checkin']=='FALSE'))]
                    df_export = df_credenciados[(df_credenciados['tp_operation']==input_empresa) & (df_credenciados['no_event']==input_edição)  & ((df_credenciados['st_checkin']==False) | (df_credenciados['st_checkin']=='FALSE'))]
                else:
                    df_credenciados = df_credenciados[(df_credenciados['tp_operation']==input_empresa) & (df_credenciados['no_event']==input_edição)]
                    df_export = df_credenciados[(df_credenciados['tp_operation']==input_empresa) & (df_credenciados['no_event']==input_edição)]
                
                df_credenciados = df_credenciados[['nu_cpf','no_credenciado','ds_function','nu_pulseira','st_checkin','st_checkout']]
                df_credenciados = df_credenciados.rename(columns={'nu_cpf': 'CPF','no_credenciado':'Nome do colaborador','ds_function':'Função','nu_pulseira':'Nro. da pulseira'})
                df_credenciados['Nro. da pulseira'] = df_credenciados['Nro. da pulseira'].astype('string')
            with st.form(key='supplier_form'):
                df = st.data_editor(
                    df_credenciados,
                    column_config={
                        "st_checkin": st.column_config.CheckboxColumn(
                            "Credenciamento",
                            help="Marque as pessoas para credenciar",
                            default=False),
                        "st_checkout": st.column_config.CheckboxColumn(
                            "Descredenciamento",
                            help="Marque para descredenciar",
                            default=False
                        )
                    },
                    disabled=['CPF'],
                    hide_index=True,
                )

                confirm_send = st.form_submit_button(label='Salvar',type='primary')
                with st.spinner('Salvando dados...'):
                    if confirm_send:
                        df['no_event'] = input_edição
                        
                        merged_df = pd.merge(df_credenciados_original, df, how='left', left_on=['nu_cpf','no_event'],right_on=['CPF','no_event'], suffixes=('_left', '_right'))
                                       
                        merged_df.loc[merged_df['CPF'].notna(), 'st_checkin_left'] = merged_df['st_checkin_right']
                        merged_df.loc[merged_df['CPF'].notna(), 'st_checkout_left'] = merged_df['st_checkout_right']
                        merged_df.loc[merged_df['CPF'].notna(), 'no_credenciado'] = merged_df['Nome do colaborador']
                        merged_df.loc[merged_df['CPF'].notna(), 'ds_function'] = merged_df['Função']
                        merged_df.loc[merged_df['CPF'].notna(), 'nu_pulseira'] = merged_df['Nro. da pulseira']

                        merged_df.loc[(((merged_df['st_checkout_left']=='True') | (merged_df['st_checkout_left']=='TRUE')) & (merged_df['time_checkout']=='')),'time_checkout'] = datetime.now() - timedelta(hours=3)
                        merged_df.loc[(((merged_df['st_checkin_left']=='True') | (merged_df['st_checkin_left']=='TRUE')) & (merged_df['time_checkin']=='')),'time_checkin'] = datetime.now() - timedelta(hours=3)
                        merged_df.loc[(((merged_df['st_checkout_left']=='False') | (merged_df['st_checkout_left']=='FALSE')) & (merged_df['time_checkout']!='')),'time_checkout'] = None
                        merged_df.loc[(((merged_df['st_checkin_left']=='False') | (merged_df['st_checkin_left']=='FALSE')) & (merged_df['time_checkin']!='')),'time_checkin'] = None                

                        merged_df = merged_df.rename(columns={'st_checkin_left': 'st_checkin','st_checkout_left':'st_checkout'})
                        merged_df = merged_df[['timestamp','no_producer','no_event','no_responsible','ds_tel_contact','ds_email_contact','no_credenciado', 'nu_cpf','ds_function','tp_operation','st_checkin','st_checkout','nu_pulseira','time_checkin','time_checkout']]
                        update_data(merged_df,spreadsheet_choice,columns)
            
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_export = df_export.rename(columns=colunmns_rename)
                df_export.to_excel(writer, sheet_name='lista_credenciamento')
                writer.close()

                st.download_button(
                    label="Exportar lista",
                    data=buffer,
                    file_name="export_credenciamento_"+str(datetime.now() - timedelta(hours=3))+".xlsx",
                    mime="application/vnd.ms-excel"
                )
    else:
        st.info('Nenhum colaborador foi cadastrado para credenciamento')

elif input_choice == 'Credenciamento':  

    spreadsheet_choice = spreadsheet_credenciamento
    columns = columns_credenciamento
    df_credenciados = pd.DataFrame(sh.worksheet(spreadsheet_choice).get_all_records())
    
    spacer1, body, spacer2 = st.columns((.1, 4, .1))
    with body:
        body= st.columns(1)
        st.header('Dados da Empresa')
        input_tp_operation = st.selectbox('Selecione sua empresa:',no_supplier['no_supplier'].sort_values(ascending=True))
        input_no_responsible = st.text_input('Nome do responsável',no_supplier[no_supplier['no_supplier']==input_tp_operation]['no_responsible'].item())
        input_nu_telefone = no_supplier[no_supplier['no_supplier']==input_tp_operation]['ds_tel_contact'].item()
        input_ds_email = no_supplier[no_supplier['no_supplier']==input_tp_operation]['ds_email_contact'].item()

        body= st.columns(1)
        with st.form(key='dei_input_form'):
            
            timestamp = datetime.now() - timedelta(hours=3)
            st.header('Dados do credenciado')
            input_no_credenciado = st.text_input('Nome do credenciado')
            input_cpf = st.text_input('CPF',max_chars=14)
            input_ds_function = st.text_input('Função')
            st.header('Dados de diversidade')
            st.info('Importante que esse formulário seja respondido pelo próprio credenciado caso possível', icon="ℹ️")

            input_nu_age = st.slider('Idade do credenciado',18,90,1)
            input_ds_gender = st.selectbox('Como você se declara quanto ao seu gênero?',tp_gender)
            input_st_trans = st.selectbox('Você se declara Trans?',tp_binary_dei)
            input_ds_race = st.selectbox('Como você se declara em relação a cor?',tp_race)
            input_st_pcd = st.selectbox('Possui alguma deficiência?',tp_binary_dei)
            confirm_send = st.form_submit_button(label='Registrar')

            if confirm_send:
                if input_tp_operation==None or input_no_responsible=='' or (input_nu_telefone=='') or (input_ds_email=='') or (input_no_credenciado=='') or (input_cpf=='') or (input_ds_function==''):
                    out_put = st.warning('Preencha todos os campos de cadastro do credenciado', icon="⚠️")
                elif (validacao_cpf(input_cpf) == False):
                    out_put = st.warning('CPF inválido...', icon="⚠️")
                elif(check_cpf_event_duplicity(input_cpf,input_edição,df_credenciados)):
                    out_put = st.warning('CPF já cadastrado para esse evento...', icon="⚠️")
                else:
                    input_cpf = input_cpf.replace('.','').replace('-','')
                    input_cpf = str(input_cpf[0:3])+'.'+str(input_cpf[3:6])+'.'+str(input_cpf[6:9])+'-'+str(input_cpf[-2:])
                    hash_input = timestamp.strftime("%d-%m-%Y")+input_no_credenciado+str(input_nu_age)+str(input_cpf)
                    
                    input_id_hash_person = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
                    input_checkin = False
                    input_checkout = False
                    input_nu_pulseira = None
                    input_time_checkin = None
                    input_time_checkout = None
                    opt1 = {
                        'timestamp':[timestamp],
                        'no_producer':[input_producer],
                        'no_event':[input_edição],
                        'no_responsible':[input_no_responsible],
                        'ds_tel_contact':[input_nu_telefone],
                        'ds_email_contact':[input_ds_email],
                        'no_credenciado':[input_no_credenciado],
                        'nu_cpf':[input_cpf],
                        'ds_function':[input_ds_function],
                        'tp_operation':[input_tp_operation],
                        'st_checkin':[input_checkin],
                        'st_checkout':[input_checkout],
                        'nu_pulseira':[input_nu_pulseira],
                        'time_checkin':[input_time_checkin],
                        'time_checkout':[input_time_checkout]
                    }
                    send_form(opt1,spreadsheet_choice,columns)

                    spreadsheet_choice = spreadsheet_dei
                    columns = columns_dei
                    input_event_concat = input_event
                    opt_general = {
                        'timestamp':[timestamp],
                        'no_producer':[input_producer],
                        'no_event':[input_event_concat],
                        'tp_operation':[input_tp_operation],
                        'id_hash_person':[input_id_hash_person],
                        'nu_age':[input_nu_age],
                        'ds_gender':[input_ds_gender],
                        'st_trans':[input_st_trans],
                        'ds_race':[input_ds_race],
                        'st_pcd':[input_st_pcd]
                    }
                    send_form(opt_general,spreadsheet_choice,columns)
                    out_put =  st.info('Registro enviado')