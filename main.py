import requests
import pyodbc
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
import os

class TokenManager:
    def __init__(self):
        self.access_token = None
        self.refresh_token = None

    def update_token(self):
        print("Reautenticando para obter novo token...")
        new_access_token, new_refresh_token = get_tokens()
        if new_access_token:
            self.access_token = new_access_token
            self.refresh_token = new_refresh_token
            print("Token atualizado com sucesso.")
        else:
            print("Falha ao atualizar token.")


# Conexão ao banco de dados
def connect_to_database():
    connection_string = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=your_server;"
        "Database=your_database;"
        "UID=your_username;"
        "PWD=your_password;"
    )
    try:
        conn = pyodbc.connect(connection_string)
        print("Conexão ao banco de dados realizada com sucesso.")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None


# Obter tokens de autenticação
def get_tokens():
    url = "https://api.userede.com.br/redelabs/oauth/token"
    body = {
        "grant_type": "your_grant_type",
        "username": "your_username",
        "password": "your_password"
    }

    headers = {
        "Authorization": "your_authorization_header=",
    }

    response = requests.post(url, data=body, headers=headers)

    if response.status_code == 200:
        data = response.json()
        access_token = data.get("access_token", "")
        refresh_token = data.get("refresh_token", "")
        return access_token, refresh_token
    else:
        print("Falha na obtenção do token. Status code:", response.status_code)
        return None, None

def fetch_installments_parallel(rows, token_manager):
    def fetch_single(row):
        row_id, parent_company_number, payment_id = row
        print(f"Buscando parcelas para Payment ID: {payment_id}, Empresa: {parent_company_number}")
        return fetch_installments_by_payment_id(token_manager, parent_company_number, payment_id), row_id

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:  # Limite para 10 threads
        results = list(executor.map(fetch_single, rows))

    return results

# Inserir dados de pagamentos no banco
def insert_payments_batch(cursor, payments):
    query = """
        INSERT INTO BD_PagamentosConsolidados (
            paymentId, paymentDate, bankCode, bankBranchCode, accountNumber,
            brandCode, parentCompanyNumber, documentNumber, companyName, tradeName,
            netAmount, status, statusCode, type, typeCode
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        for payment in payments:
            print(f"    Inserindo pagamento ID: {payment.get('paymentId')}")
        cursor.executemany(query, [
            (
                payment.get('paymentId', None),
                payment.get('paymentDate', None),
                payment.get('bankCode', None),
                payment.get('bankBranchCode', None),
                payment.get('accountNumber', None),
                payment.get('brandCode', None),
                payment.get('companyNumber', None),
                payment.get('documentNumber', None),
                payment.get('companyName', None),
                payment.get('tradeName', None),
                payment.get('netAmount', 0.0),
                payment.get('status', None),
                payment.get('statusCode', None),
                payment.get('type', None),
                payment.get('typeCode', None)
            ) for payment in payments
        ])
    except Exception as e:
        print(f"Erro ao inserir pagamentos em batch: {e}")


# Atualizar parcelas no banco de dados
def update_installments_batch(cursor, installments, row_ids):
    query = """
        UPDATE BD_PagamentosConsolidados
        SET
            installmentQuantity = ?, 
            installmentNumber = ?, 
            saleAmount = ?, 
            authorizationCode = ?, 
            brand = ?, 
            cardNumber = ?, 
            expirationDate = ?, 
            flexFee = ?, 
            mdrAmount = ?, 
            feeTotal = ?, 
            nsu = ?
        WHERE id = ?
    """
    try:
        cursor.executemany(query, [
            (
                installment.get('installmentQuantity', 0),
                installment.get('installmentNumber', 0),
                installment.get('saleAmount', 0.0),
                installment.get('authorizationCode', "N/A"),
                installment.get('brand', "N/A"),
                installment.get('cardNumber', "N/A"),
                installment.get('expirationDate', None),
                installment.get('flexFee', 0.0),
                installment.get('mdrAmount', 0.0),
                installment.get('feeTotal', 0.0),
                installment.get('nsu', 0),
                row_id
            ) for installment, row_id in zip(installments, row_ids)
        ])
    except Exception as e:
        print(f"Erro ao atualizar parcelas em batch: {e}")


# Processar pagamentos para uma única empresa em um dia
def process_company_payments(company_number, token_manager, day, conn_string, batch_size):
    print(f"  Processando empresa: {company_number} para o dia {day}")
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()
    url = "https://api.userede.com.br/redelabs/merchant-statement/v1/payments"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token_manager.access_token}"}

    params = {
        "startDate": day,
        "endDate": day,
        "parentCompanyNumber": company_number,
        "subsidiaries": company_number,
        "pageKey": None
    }

    payments_batch = []

    while True:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if 'content' in data and 'payments' in data['content']:
                payments = data['content']['payments']
                payments_batch.extend(payments)

                if len(payments_batch) >= batch_size:
                    insert_payments_batch(cursor, payments_batch[:batch_size])
                    payments_batch = payments_batch[batch_size:]

                if 'cursor' in data and data['cursor'].get('hasNextKey', False):
                    params['pageKey'] = data['cursor']['nextKey']
                else:
                    break
            else:
                break
        elif response.status_code == 401:
            print(f"Token expirado para empresa {company_number}. Reautenticando...")
            token_manager.update_token()
            headers["Authorization"] = f"Bearer {token_manager.access_token}"
        else:
            print(f"Erro na requisição para empresa {company_number}: {response.status_code}")
            break

    if payments_batch:
        insert_payments_batch(cursor, payments_batch)

    conn.commit()
    conn.close()


# Processar pagamentos e parcelas para todas as empresas de um dia
def process_daily_payments_and_installments(day, company_numbers, access_token, conn_string, batch_size):
    print(f"Iniciando processamento para o dia: {day}")

    # Processar pagamentos (mantém o mesmo)
    with ProcessPoolExecutor() as executor:
        tasks = [
            executor.submit(process_company_payments, company_number, access_token, day, conn_string, batch_size)
            for company_number in company_numbers
        ]
        for task in tasks:
            task.result()

    # Buscar parcelas paralelamente
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()
    print(f"Buscando parcelas para os pagamentos do dia: {day}")

    cursor.execute("""
        SELECT id, parentCompanyNumber, paymentId 
        FROM BD_PagamentosConsolidados 
        WHERE paymentDate = ?
    """, (day,))
    rows = cursor.fetchall()

    installments_results = fetch_installments_parallel(rows, access_token)

    # Atualizar parcelas no banco
    installments_batch = []
    row_ids = []
    for installments_data, row_id in installments_results:
        if installments_data and 'content' in installments_data and 'installments' in installments_data['content']:
            for installment in installments_data['content']['installments']:
                installments_batch.append(installment)
                row_ids.append(row_id)

                if len(installments_batch) >= batch_size:
                    update_installments_batch(cursor, installments_batch[:batch_size], row_ids[:batch_size])
                    installments_batch = installments_batch[batch_size:]
                    row_ids = row_ids[batch_size:]

    if installments_batch:
        update_installments_batch(cursor, installments_batch, row_ids)

    conn.commit()
    conn.close()

def fetch_installments_by_payment_id(token_manager, parent_company_number, payment_id):
    url = f"https://api.userede.com.br/redelabs/merchant-statement/v2/payments/installments/{parent_company_number}/{payment_id}"
    headers = {"Authorization": f"Bearer {token_manager.access_token}", "Content-Type": "application/json"}
    
    retries = 3  # Número de tentativas
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 401:
                print("Token expirado ao buscar parcelas. Reautenticando...")
                token_manager.update_token()
                headers["Authorization"] = f"Bearer {token_manager.access_token}"
                response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Erro na API de parcelas por Payment ID. Status code: {response.status_code}")
                return None
        except requests.exceptions.Timeout:
            print(f"Tentativa {attempt + 1} de {retries} falhou devido a timeout. Retentando...")
        except requests.exceptions.ConnectionError as e:
            print(f"Tentativa {attempt + 1} de {retries} falhou devido a erro de conexão: {e}. Retentando...")
    print("Falha após múltiplas tentativas.")
    return None



# Principal
def main():
    conn_string = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=your_server;"
        "Database=your_database;"
        "UID=your_username;"
        "PWD=your_password;"
    )

    conn = connect_to_database()
    if not conn:
        return

    start_date = input("Digite a data inicial (YYYY-MM-DD): ")
    end_date = input("Digite a data final (YYYY-MM-DD): ")
    batch_size = 5  # Configuração do tamanho do batch

    company_numbers = [
            12345678,  # Insira os números das empresas aqui
            ]

    token_manager = TokenManager()
    token_manager.update_token()  # Inicializar o token

    if not token_manager.access_token:
        print("Erro ao obter token de autenticação.")
        return

    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

    date_range = [
        (start_date_obj + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end_date_obj - start_date_obj).days + 1)
    ]

    for day in date_range:
        process_daily_payments_and_installments(day, company_numbers, token_manager, conn_string, batch_size)

    print("Processo concluído.")



if __name__ == "__main__":
    main()
