import mysql.connector
import requests
from requests.structures import CaseInsensitiveDict
import re

#Funções de Banco de Dados ------------------------------------------

objConexao = mysql.connector.connect(host='botuni9.c3cupjqiyqbn.sa-east-1.rds.amazonaws.com', database='ChatBot', user='admin', password='7pPdu#GSX.2sYG')

def selectBanco(objConexao, strQuery):
    
    if objConexao.is_connected():
        cursor = objConexao.cursor()
        cursor.execute(strQuery)
        resultado = cursor.fetchall()

        cursor.close()

        return resultado

def insertUpdateDeleteBanco(objConexao, strQuery):

    if objConexao.is_connected():
        cursor = objConexao.cursor()
        cursor.execute(strQuery)
        objConexao.commit()

        cursor.close()

def guardaStatusMetro():
    tabBancoDados = selectBanco(objConexao, "SELECT NOMELINHA FROM STATUS_METRO WHERE CODIGO = '1' AND DTATUALIZACAO >= CURRENT_TIMESTAMP()-500;")

    if len(tabBancoDados) == 0:
        try:
            request = requests.get("https://www.diretodostrens.com.br/api/status")
            conteudo = request.json()

            for linha in conteudo:
                try:
                    insertUpdateDeleteBanco(objConexao, "UPDATE STATUS_METRO SET SITUACAO = '" + str(linha['situacao']) +  "', DTATUALIZACAO = CURRENT_TIMESTAMP, DESCRICAO = '" + str(linha['descricao']) +  "' WHERE CODIGO = '" + str(linha['codigo']) +  "';")
                except:
                    insertUpdateDeleteBanco(objConexao, "UPDATE STATUS_METRO SET SITUACAO = '" + str(linha['situacao']) +  "', DTATUALIZACAO = CURRENT_TIMESTAMP, DESCRICAO = NULL WHERE CODIGO = '" + str(linha['codigo']) +  "';")
        except:
            insertUpdateDeleteBanco(objConexao, "INSERT INTO LOG (RETORNO, ETAPA) VALUES ('Ocorreu um erro ao buscar dados da API direto dos trens', 'Erro');")

def enviaMsg(strChatId, strMensagem, tabBancoDados):
    try:
        resposta = "null"

        strMensagem = substituiVariaveisMensagem(strChatId, strMensagem)
        
        url = "https://api.telegram.org/bot5751250760:AAG6Fs7zjgKKG8u9S_1BkO53Tn6z5u5C4XI/sendMessage"

        headers = CaseInsensitiveDict()
        headers["Content-Type"] = "application/json"

        strBotoes = ''
        if len(tabBancoDados) == 0:
            data = '{"chat_id":"' + strChatId + '","text":"' + strMensagem + '", "reply_markup":{"remove_keyboard":true}}'
        else:

            data = '{"chat_id":"' + strChatId + '","text":"' + strMensagem + '","reply_markup":{"keyboard":['

            for linha in tabBancoDados:
                strBotoes = strBotoes + '[{"text":"'+ linha[0] +'"}]' + ","

            data = data + strBotoes
            data = data + ']}}'

            data = data.replace(",]}}", "]}}")

        data = data.encode("utf-8")

        resposta = requests.post(url, headers=headers, data=data)
    except:
        insertUpdateDeleteBanco(objConexao, "INSERT INTO LOG (RETORNO, ETAPA) VALUES ('Ocorreu um erro na função EnviaMsg', 'Erro');")

    return resposta

def entraFluxoConversa(strChatId, strIDFluxo):
    strIDFluxo = str(strIDFluxo)
    #registrar na tabela CONTATO_FLUXOATUAL o fluxo
    insertUpdateDeleteBanco(objConexao, "DELETE FROM CONTATO_FLUXOATUAL WHERE IDCTT = '" + strChatId + "';")
    insertUpdateDeleteBanco(objConexao, "INSERT INTO CONTATO_FLUXOATUAL (IDCTT, IDFLUXOATUAL, IDNUMSEQATUAL) VALUES ('" + strChatId + "', " + str(strIDFluxo) + ", 1);")
    tabBancoDados       = selectBanco(objConexao, "SELECT MENSAGEM   FROM FLUXOS_MENSAGENS WHERE IDFLUXO = '" + strIDFluxo + "' AND NUMSEQ = '1';")
    tabBancoDadosBotoes = selectBanco(objConexao, "SELECT RESPACEITA FROM FLUXOS_RESPOSTAS WHERE IDFLUXO = '" + strIDFluxo + "' AND NUMSEQ = '1' AND BOTAO = 'S';")

    enviaMsg(strChatId, str(tabBancoDados[0][0]), tabBancoDadosBotoes)

def continuaFluxo(strChatId, strFluxoAtual, strSequenciaAtual):
    strSequenciaAtual = int(strSequenciaAtual) + 1
    strSequenciaAtual = str(strSequenciaAtual)
    #Busca a mensagem da sequencia atual
    tabBancoDadosFluxo = selectBanco(objConexao,  "SELECT MENSAGEM   FROM FLUXOS_MENSAGENS WHERE IDFLUXO = '" + strFluxoAtual + "' AND NUMSEQ = '" + strSequenciaAtual +  "';")
    tabBancoDadosBotoes = selectBanco(objConexao, "SELECT RESPACEITA FROM FLUXOS_RESPOSTAS WHERE IDFLUXO = '" + strFluxoAtual + "' AND NUMSEQ = '" + strSequenciaAtual +  "' AND BOTAO = 'S';")

    if len(tabBancoDadosFluxo) > 0:
        #atualiza para a proxima mensagem
        insertUpdateDeleteBanco(objConexao, "UPDATE CONTATO_FLUXOATUAL SET IDNUMSEQATUAL = '" + strSequenciaAtual + "', DTATUALIZACAO = CURRENT_TIMESTAMP WHERE IDCTT = '" + strChatId + "'")
        enviaMsg(strChatId, str(tabBancoDadosFluxo[0][0]), tabBancoDadosBotoes)
    else:
        entraFluxoConversa(strChatId, "1")

def substituiVariaveisMensagem(strChatId, strMensagem):
    #Substitui as chaves/variaveis que estão na mensagem por informações que estão no banco de dados
    if strMensagem.count('[Nome]'):
        tabVariavelMsg = selectBanco(objConexao, "SELECT NOMECTT FROM MENSAGENS_RECEBIDAS WHERE IDCTT = '" + strChatId + "' ORDER BY IDMSG DESC LIMIT 1;")
        strMensagem = strMensagem.replace("[Nome]", tabVariavelMsg[0][0])

    if strMensagem.count('[Linha]'):
        tabBancoDados = selectBanco(objConexao, "SELECT MENSAGEM FROM MENSAGENS_RECEBIDAS WHERE IDCTT = '" + strChatId + "' ORDER BY IDMSG DESC LIMIT 1;")
        strLinha = tabBancoDados[0][0]
        strLinha = re.sub('[^0-9]', '', strLinha)
        tabVariavelMsg = selectBanco(objConexao, "SELECT CODIGO, NOMELINHA, SITUACAO, DATE_FORMAT(DTATUALIZACAO, '%d/%m às %Hh%i') AS DT, DESCRICAO FROM STATUS_METRO WHERE CODIGO = '" + str(strLinha) + "';")
        strCodLinha = (str(tabVariavelMsg[0][0]) + ' - ' + tabVariavelMsg[0][1])
        strMensagem = strMensagem.replace("[Linha]", strCodLinha)

        if str(tabVariavelMsg[0][4]) == "None":
            strMensagem = strMensagem.replace("[Status]", tabVariavelMsg[0][2])
        else:
            strMensagem = strMensagem.replace("[Status]", tabVariavelMsg[0][4])

        strMensagem = strMensagem.replace("[DtAtualizacao]", str(tabVariavelMsg[0][3]))

    return strMensagem

def substituiSituacaoMensagem(strMensagem, strLinha):
    if strMensagem.count('[LinhaCadastrada]'):
        strLinha = re.sub('[^0-9]', '', strLinha)
        tabVariavelMsg = selectBanco(objConexao, "SELECT CODIGO, NOMELINHA, SITUACAO, DATE_FORMAT(DTATUALIZACAO, '%d/%m às %Hh%i') AS DT, DESCRICAO FROM STATUS_METRO WHERE CODIGO = '" + str(strLinha) + "';")
        strCodLinha = (str(tabVariavelMsg[0][0]) + ' - ' + tabVariavelMsg[0][1])
        strMensagem = strMensagem.replace("[LinhaCadastrada]", strCodLinha)

        if str(tabVariavelMsg[0][4]) == "None":
            strMensagem = strMensagem.replace("[Status]", tabVariavelMsg[0][2])
        else:
            strMensagem = strMensagem.replace("[Status]", tabVariavelMsg[0][4])

        strMensagem = strMensagem.replace("[DtAtualizacao]", str(tabVariavelMsg[0][3]))

    return strMensagem

insertUpdateDeleteBanco(objConexao, "INSERT INTO LOG (RETORNO, ETAPA) VALUES ('JOB iniciado', 'Sucesso');")

guardaStatusMetro()

tabContatosAgendados = selectBanco(objConexao, "SELECT DISTINCT IDCTT FROM CONTATO_AGENDAMENTOS WHERE HOUR(HORA) = HOUR(CURRENT_TIME());")

if len(tabContatosAgendados) > 0:

    tabContatosLinhasAgendadas = selectBanco(objConexao, "SELECT IDLINHA, IDCTT FROM CONTATO_LINHA WHERE IDCTT IN (SELECT IDCTT FROM CONTATO_AGENDAMENTOS WHERE HOUR(HORA) = HOUR(CURRENT_TIME())) ORDER BY IDLINHA;")
    tabMensagemEnviar          = selectBanco(objConexao, "SELECT NUMSEQ, MENSAGEM FROM FLUXOS_MENSAGENS WHERE IDFLUXO = 8 ORDER BY NUMSEQ;")

    for linha in tabContatosAgendados:
        strChatId = linha [0]

        for mensagens in tabMensagemEnviar:
            strMensagem = mensagens[1]

            if str(mensagens[0]) == '2':

                for linhasAgendadas in tabContatosLinhasAgendadas:
                    strMensagem = mensagens[1]
                    strLinha = str(linhasAgendadas[0])
                    strMensagem = substituiSituacaoMensagem(strMensagem, strLinha)
                    enviaMsg(strChatId, strMensagem, '')

            else:

                enviaMsg(strChatId, strMensagem, '')


