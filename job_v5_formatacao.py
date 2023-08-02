# Criação de Pipeline de Extração, Limpeza, Transformação e Enriquecimento de Dados

# 1 - Carregar somente registros com quantidade produzida superior a 10
# 2 - Remover o carácter "ponto" na última coluna para evitar que o número seja truncado
# 3 - Regra de negócio: enriquecer os dados adicionando no destino uma coluna com a mergem de lucro de cada produto.
# 4 - Formatando os dados para duas casas decimais.


import csv
import sqlite3


# Função para remover o ponto
def remove_ponto(valor):
    return int(round(float(valor.replace('.', '')), 0))


# Abre o arquivo CSV para leitura
with open('producao_alimentos.csv', 'r') as arquivo:
    # Cria um leitor para ler o CSV
    leitor = csv.reader(arquivo)

    # Pula a primeira linha, que contém os cabeçalhos da colunas
    next(leitor)

    # Conecta ao banco de dados
    conex = sqlite3.connect('prod_alimentos.db')

    # Deleta a tabela se existir no banco de dados
    conex.execute('DROP TABLE IF EXISTS producao')

    # Cria uma tabela para armazenar os dados com a nova coluna "margem_lucro"
    conex.execute('''CREATE TABLE producao (
    produto TEXT, 
    quantidade INTEGER,
    preco_medio REAL,
    receita_total INTEGER,
    margem_lucro REAL
    )''')

    # 8 - Resolvendo os problemas de negócio:

    for linha in leitor:
        if int(linha[1]) > 10:
            # Remove o ponto do valor da última coluna e converte para inteiro
            linha[3] = remove_ponto(linha[3])

            # Calculando a margem de lucro bruta com base no valor médio de venda e na receita total
            margem_lucro = round((linha[3] / float(linha[1])) - float(linha[2]))

            # Insere a linha com a nova coluna "margem_lucro" na tabela do banco de dados
            conex.execute(
                'INSERT INTO producao (produto, quantidade, preco_medio, receita_total, margem_lucro) VALUES (?,?,?,?,?)',
                (linha[0], linha[1], linha[2], linha[3], margem_lucro))

    conex.commit()
    conex.close()

print("Job concluído com sucesso!")
