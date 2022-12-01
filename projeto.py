#Importação de biblotecas -------------------------------------------

from flask import Flask, request
import requests, mysql.connector
from requests.structures import CaseInsensitiveDict
import re

app = Flask(__name__)

#Conexao com o banco de dados ---------------------------------------

objConexao = mysql.connector.connect(host='botuni9.c3cupjqiyqbn.sa-east-1.rds.amazonaws.com', database='ChatBot', user='admin', password='7pPdu#GSX.2sYG')

#Algoritmo da API ---------------------------------------------------

@app.route('/webhook', methods = ['POST'])
def webhook():

    try:
        #Transforma a string JSON numa variável tipo dicionadio do Python
        dicionario = request.get_json()

        #Coleta dados da mensagem
        strMensagem  = str(dicionario['message']['text'])
        strNome      = str(dicionario['message']['from']['first_name'])
        strChatId    = str(dicionario['message']['from']['id'])

        #Guarda a mensagem no Banco de dados
        guardaMensagem(strChatId, strMensagem, strNome)

        #Verifica se é a primeira mensagem que o usuario envia para o bot
        bPrimeiraMensagem = True
        tabFluxoAtual     = retornaFluxoAtual(strChatId)

        if len(tabFluxoAtual) > 0:        
            bPrimeiraMensagem = False
            strFluxoAtual     = str(tabFluxoAtual[0][0])
            strSequenciaAtual = str(tabFluxoAtual[0][1])

        #Separa os caminhos caso seja a primeira mensagem ou caso o usuario já esteve em uma conversa
        if bPrimeiraMensagem:
            #Verifica se o usuário está cadastrado
            bExisteCadastro = verificaCadastro(strChatId)

            #Separa os caminhos caso, se o usuário estiver cadastrado será enviada uma mensagem, se não estiver será enviado outra
            if bExisteCadastro:
                #Entra no primeiro fluxo de mensagem
                entraFluxoConversa(strChatId, "1")
            else:
                #Entra no primeiro fluxo de mensagem
                entraFluxoConversa(strChatId, "4")

        else:
            #Se não for a primeira mensagem, irá continua uma conversa
            #Traz todas as respostas aceitas daquela sequencia/fluxo que o usuário está
            tabRespostas = selectBanco(objConexao, "SELECT RESPACEITA, IDFLUXOREDIREC, VALOR, REPETIR FROM FLUXOS_RESPOSTAS WHERE IDFLUXO = '" + strFluxoAtual + "' AND NUMSEQ = '" + strSequenciaAtual + "';")
            bRespostaAceita = False

            if len(tabRespostas) > 0:
                #Loop nas respostas aceitas verificando se ela é igual a resposta recebida
                for linha in tabRespostas:
                    strResposta          = linha[0]
                    strRedirecionarFluxo = linha[1]
                    strValorMsg          = str(linha[2])
                    strRepetirMsg        = str(linha[3])

                    if strResposta == '*':
                        bRespostaAceita = True
                        break
                    elif strMensagem.upper() == strResposta.upper():
                        bRespostaAceita = True
                        break
                
                #Se a resposta
                if bRespostaAceita:
                    if strRepetirMsg == 'S':
                        continuaFluxo(strChatId, strFluxoAtual, int(strSequenciaAtual)-2)
                    
                    elif strRedirecionarFluxo == None:
                        #Responde e continua o fluxo
                        if strFluxoAtual == '6':
                            guardaStatusMetro()

                        #Apaga o horario e rota do contato se ele estiver no fluxo de mudança de rota
                        if (strFluxoAtual == '2' and strSequenciaAtual == '1') or (strFluxoAtual == '7' and strSequenciaAtual == '1'):
                            insertUpdateDeleteBanco(objConexao, "DELETE FROM CONTATO_AGENDAMENTOS WHERE IDCTT = '" + strChatId + "';")
                            insertUpdateDeleteBanco(objConexao, "DELETE FROM CONTATO_LINHA        WHERE IDCTT = '" + strChatId + "';")

                        #Cadastra a linha para o contato se estiver em algum fluxo de cadastro
                        if (strFluxoAtual == '5' and strSequenciaAtual == '1') or (strFluxoAtual == '7' and (strSequenciaAtual == '1' or strSequenciaAtual == '2')):
                            insertUpdateDeleteBanco(objConexao, "INSERT INTO CONTATO_LINHA (IDCTT, IDLINHA) VALUES ('" + strChatId + "', '" + strValorMsg + "');")

                        #Cadastra o horario para o contato se estiver em algum fluxo de cadastro
                        if (strFluxoAtual == '5' and strSequenciaAtual == '3') or (strFluxoAtual == '7' and strSequenciaAtual == '3'):
                            insertUpdateDeleteBanco(objConexao, "INSERT INTO CONTATO_AGENDAMENTOS (IDCTT, HORA) VALUES ('" + strChatId + "', '" + strMensagem + "');")
                            bExisteCadastro = True
            
                        continuaFluxo(strChatId, strFluxoAtual, strSequenciaAtual)
                    else:
                        #Redireciona para o fluxo de acordo com o cadastro do banco
                        if strRedirecionarFluxo == 1:
                            bExisteCadastro = verificaCadastro(strChatId)

                            if bExisteCadastro:
                                entraFluxoConversa(strChatId, "1")
                            else:
                                entraFluxoConversa(strChatId, "4")
                        else:
                            entraFluxoConversa(strChatId, strRedirecionarFluxo)

                else:
                    enviaMsg(strChatId, 'Nao entendi sua resposta, por favor responda corretamente usando os botões abaixo', '')
            
            else:
                #Verifica se o usuário está cadastrado
                bExisteCadastro = verificaCadastro(strChatId)

                #Se a mensagem atual não tiver nenhuma resposta aceita, então o bot volta para o fluxo de menú
                if bExisteCadastro:
                    entraFluxoConversa(strChatId, "1")
                else:
                    entraFluxoConversa(strChatId, "4")
    except:
        return "ERROR"

    return "OK"

#Funções de fluxo ---------------------------------------------------

def verificaCadastro(strChatId):
    bExisteCadastro = False
    
    tabBancoDados = selectBanco(objConexao, "SELECT * FROM CONTATO_AGENDAMENTOS WHERE IDCTT = '" + strChatId + "';")

    if len(tabBancoDados) > 0:
        bExisteCadastro = True
    
    return bExisteCadastro

def guardaMensagem(strChatId, strMensagem, strNomeCtt):
    insertUpdateDeleteBanco(objConexao, "INSERT INTO MENSAGENS_RECEBIDAS (IDCTT, MENSAGEM, NOMECTT) VALUES ('" + strChatId + "', '" + strMensagem + "', '" + strNomeCtt + "');")

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

def retornaFluxoAtual(strChatId):
    tabBancoDados = selectBanco(objConexao, "SELECT IDFLUXOATUAL, IDNUMSEQATUAL FROM CONTATO_FLUXOATUAL WHERE IDCTT = '" + strChatId + "' AND DTATUALIZACAO >= CURRENT_TIMESTAMP()-500;")

    return tabBancoDados

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

#Funções de Banco de Dados ------------------------------------------

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

if __name__ == "__main__":
    app.run(debug = True)