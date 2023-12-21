# Projeto Final – Banco de Dados
import psycopg2
import csv

# Abertura da conexão com banco de dados
# Abertura do cursor
try:
    conn = psycopg2.connect("host=localhost dbname=ProjetoFinal user=postgres password=123")
    print("Conexão realizada")
    cur = conn.cursor()

    # Criação de tabela
    cur.execute('''
        CREATE TABLE IF NOT EXISTS regioes (
            id SERIAL PRIMARY KEY,
            noc CHAR(3),
            nome_regiao VARCHAR(100),
            notas VARCHAR(100),
            CONSTRAINT unico_noc_regiao UNIQUE (noc, nome_regiao)
        );
    ''')

    # Abertura de arquivo csv
    try:
        with open('noc_regions.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)

            # Inserção de dados na tabela regioes
            for row in reader:
                noc = row[0]
                nome_regiao = row[1]
                notas = row[2]
                
                cur.execute('''INSERT INTO regioes (noc, nome_regiao, notas) VALUES (%s, %s, %s) ON CONFLICT ON CONSTRAINT unico_noc_regiao DO NOTHING;''', (noc, nome_regiao, notas))
    
    except FileNotFoundError:
        print("O arquivo mencionado não foi encontrado!")   

    # Criação de tabela
    cur.execute('''
        CREATE TABLE IF NOT EXISTS times (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100),
            pais VARCHAR(100)
        );
    ''')

    # Criação de tabela
    cur.execute('''
        CREATE TABLE IF NOT EXISTS atletas (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100),
            sexo VARCHAR(1),
            idade INT,
            equipe VARCHAR(100),
            ano_jogos INT,
            temporada VARCHAR(6),
            id_time INT,
            FOREIGN KEY(id_time) REFERENCES times(id)
        );
    ''')

    # Criação de tabela
    cur.execute('''
        CREATE TABLE IF NOT EXISTS medalhas (
            id SERIAL PRIMARY KEY,
            id_atleta INT,
            medalha VARCHAR(10),
            FOREIGN KEY(id_atleta) REFERENCES atletas(id)
        );
    ''')

    # Criação de tabela
    cur.execute('''
        CREATE TABLE IF NOT EXISTS modalidades (
            id SERIAL PRIMARY KEY,
            nome_modalidade VARCHAR(100),
            ano_introducao INT
        );
    ''')

    # Criação de tabela
    cur.execute('''
        CREATE TABLE IF NOT EXISTS esportes (
            id SERIAL PRIMARY KEY,
            id_modalidade INT,
            nome_esporte VARCHAR(100),
            id_regiao INT,
            FOREIGN KEY(id_modalidade) REFERENCES modalidades(id),
            FOREIGN KEY(id_regiao) REFERENCES regioes(id)
        );
    ''') 

    # Abertura de arquivo csv
    try:
        with open('athlete_events.csv', 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)

            # Inserção de dados nas tabelas times, atletas e medalhas
            for row in reader:
                nome = row[1]
                sexo = row[2]
                idade = int(row[3]) if row[3] != 'NA' else None
                equipe = row[6]
                noc = row[7]
                ano_jogos = row[9]
                temporada = row[10]
                medalha = row[14]

                cur.execute('''INSERT INTO times (nome, pais) VALUES (%s, %s) ON CONFLICT DO NOTHING;''', (equipe, noc))
                cur.execute('''SELECT id FROM times WHERE nome = %s;''', (equipe,))
                id_equipe = cur.fetchone()[0]

                cur.execute('''INSERT INTO atletas (nome, idade, sexo, id_time, ano_jogos, temporada) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;''', (nome, idade, sexo, id_equipe, ano_jogos, temporada))
                id_atleta = cur.fetchone()[0]

                cur.execute('''INSERT INTO medalhas (id_atleta, medalha) VALUES (%s, %s)''', (id_atleta, medalha))

    except FileNotFoundError:
        print("O arquivo mencionado não foi encontrado!")


    # Commit das alterações no banco de dados
    conn.commit()
    print("Tabelas criadas!")

################################################################################

    # Consulta 1
    cur.execute('''
        SELECT a.equipe, COUNT(medalha) AS total_medalhas
        FROM atletas a INNER JOIN medalhas m
        ON a.id = m.id_atleta
        WHERE a.ano_jogos >= 1990 AND m.medalha != 'NA'
        GROUP BY a.equipe
        ORDER BY a.equipe DESC;
    ''')
    result_1 = cur.fetchall()
    print("Total de medalhas por país desde 1990:")
    for row in result_1:
        noc, total_medals = row
        print(f"País: {noc}\nTotal de Medalhas: {total_medals}")



    # Consulta 2
    cur.execute('''
        SELECT a.nome, COALESCE(SUM(CASE WHEN m.medalha = 'Gold' THEN 1 ELSE 0 END), 0) AS total_ouro
        FROM atletas a
        INNER JOIN medalhas m ON m.id_atleta = a.id
        GROUP BY a.nome
        ORDER BY total_ouro DESC
        LIMIT 3;
    ''')
    result_2a = cur.fetchall()
    print("TOP 3 atletas com mais medalhas de ouro:")
    for row in result_2a:
        print(row)

    cur.execute('''
        SELECT a.nome, COALESCE(SUM(CASE WHEN m.medalha = 'Silver' THEN 1 ELSE 0 END), 0) AS total_prata
        FROM atletas a
        INNER JOIN medalhas m ON m.id_atleta = a.id
        GROUP BY a.nome
        ORDER BY total_prata DESC
        LIMIT 3;
    ''')
    result_2b = cur.fetchall()
    print("\nTOP 3 atletas com mais medalhas de prata:")
    for row in result_2b:
        print(row)

    cur.execute('''
        SELECT a.nome, COALESCE(SUM(CASE WHEN m.medalha = 'Bronze' THEN 1 ELSE 0 END), 0) AS total_bronze
        FROM atletas a
        INNER JOIN medalhas m ON m.id_atleta = a.id
        GROUP BY a.nome
        ORDER BY total_bronze DESC
        LIMIT 3;
    ''')
    result_2c = cur.fetchall()
    print("\nTOP 3 atletas com mais medalhas de bronze:")
    for row in result_2c:
        print(row)



    # Consulta 3
    cur.execute('''
        SELECT nome_modalidade, MIN(ano_introducao) AS ano_introducao
        FROM modalidades
        GROUP BY nome_modalidade;
    ''')
    result_3 = cur.fetchall()
    print("Modalidades e ano de introdução:")
    for row in result_3:
        nome_modalidade, ano_introducao = row
        print(f"Modalidade: {nome_modalidade}\nAno de Introdução: {ano_introducao}")



    # Consulta 4
    cur.execute('''
        SELECT m.medalha AS quantidade_ouro
        FROM atletas a
        INNER JOIN times t ON a.id_time = t.id
        INNER JOIN medalhas m ON m.id_atleta = a.id
        INNER JOIN esportes e ON e.id_regiao = t.id
        WHERE e.nome_esporte = 'Volleyball' AND m.medalha = 'Gold'
        GROUP BY t.pais;
    ''')
    result_4a = cur.fetchall()
    print("Medalhas de ouro no vôlei por país:")
    print(result_4a)

    cur.execute('''
        SELECT m.medalha AS quantidade_prata
        FROM atletas a
        INNER JOIN times t ON a.id_time = t.id
        INNER JOIN medalhas m ON m.id_atleta = a.id
        INNER JOIN esportes e ON e.id_regiao = t.id
        WHERE e.nome_esporte = 'Volleyball' AND m.medalha = 'Silver'
        GROUP BY t.pais;
    ''')
    result_4b = cur.fetchall()
    print("Medalhas de prata no vôlei por país:")
    print(result_4b)

    cur.execute('''
        SELECT m.medalha AS quantidade_bronze
        FROM atletas a
        INNER JOIN times t ON a.id_time = t.id
        INNER JOIN medalhas m ON m.id_atleta = a.id
        INNER JOIN esportes e ON e.id_regiao = t.id
        WHERE e.nome_esporte = 'Volleyball' AND m.medalha = 'Bronze'
        GROUP BY t.pais;
    ''')
    result_4c = cur.fetchall()
    print("Medalhas de bronze no vôlei por país:")
    print(result_4c)



    # Consulta 5
    cur.execute('''
        SELECT ano_jogos, temporada, AVG(total_atletas) AS media_atletas
        FROM (
            SELECT ano_jogos, temporada, COUNT(*) AS total_atletas
            FROM atletas
            WHERE ano_jogos >= 1920
            GROUP BY ano_jogos, temporada
            ) AS subquery
        GROUP BY ano_jogos, temporada
        ORDER BY ano_jogos;
    ''')
    result_5 = cur.fetchall()
    print("Média de atletas por ano desde 1920:")
    for row in result_5:
        ano_jogos, temporada, media_atletas = row
        print(f"Ano dos Jogos: {ano_jogos}, Temporada: {temporada}, Média de Atletas: {media_atletas}")



    # Consulta 6
    cur.execute('''
        SELECT sexo, 
            CASE WHEN a.ano_jogos < 1950 THEN 'Antes de 1950' ELSE 'Depois de 1950' END AS periodo,
            COUNT(*) AS quantidade_atletas,
            ROUND(CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY a.ano_jogos) AS NUMERIC), 2) AS proporcao
        FROM atletas a
        WHERE a.ano_jogos >= 1920
        GROUP BY periodo, sexo, a.ano_jogos
        ORDER BY periodo, sexo;
    ''')
    result_6 = cur.fetchall()
    print("Proporção de homens e mulheres antes e depois de 1950:")
    for row in result_6:
        periodo, sexo, quantidade_atletas, proporcao = row
        print(f"Período: {periodo}, Sexo: {sexo}, Quantidade: {quantidade_atletas}, Proporção: {proporcao}%")


    # A consulta 6 conta o número de homens e mulheres em cada período
    # Depois calcula a proporção de cada sexo em relação ao total de atletas
    # Esta análise dá a entender que houve mudanças na representação de mulheres nos jogos antes e depois de 1950

    # Commit das alterações no banco de dados
    # Fechamento do cursor
    # Fechamento da conexão com o banco de dados
    conn.commit()
    cur.close()
    conn.close()

except psycopg2.Error as e:
    print("Conexão falhou:", e)