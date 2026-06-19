# Importa o módulo para criar e gerenciar múltiplas threads (tarefas simultâneas)
import threading
# Importa o módulo para pausar a execução (usado para simular o tempo de processamento e forçar erros)
import time
# Importa o módulo para gerar números aleatórios (usado para simular lag de rede)
import random
# Importa uma Fila nativamente segura para threads (thread-safe) para organizar os pedidos
from queue import Queue

#  MEMÓRIA COMPARTILHADA: o saldo da conta
#  A classe que será protegida pelo Lock
class ContaBancaria:
    def __init__(self, titular, saldo_inicial):
        self.titular = titular
        self.saldo = saldo_inicial
        # LOCK: Cria o "cadeado". É ele que impede que duas threads acessem o saldo ao mesmo tempo.
        self.lock = threading.Lock()          
        self.historico = []                   

    def depositar(self, valor, origem):
        # O comando 'with' adquire o lock (tranca a porta). Outras threads esperam aqui até ser liberado.
        with self.lock:                        
            saldo_antes = self.saldo
            # O sleep simula o tempo que o banco leva para processar, com lock para não ocorrer erros
            time.sleep(0.01)                   
            self.saldo += valor
            registro = f"[DEPÓSITO]  {origem:20s} | +R${valor:7.2f} | Saldo: R${self.saldo:.2f}"
            self.historico.append(registro)
            print(f"  {registro}")
        # Ao sair do bloco 'with', o lock é liberado automaticamente.
        return True

    def sacar(self, valor, origem):
        # Novamente, trancamos a porta para verificar e debitar o saldo com segurança.
        with self.lock:                        
            if self.saldo >= valor:
                saldo_antes = self.saldo
                time.sleep(0.01)               
                self.saldo -= valor
                registro = f"[SAQUE]     {origem:20s} | -R${valor:7.2f} | Saldo: R${self.saldo:.2f}"
                self.historico.append(registro)
                print(f"  {registro}")
                return True
            else:
                registro = f"[RECUSADO]  {origem:20s} | -R${valor:7.2f} | Saldo insuficiente (R${self.saldo:.2f})"
                self.historico.append(registro)
                print(f"  {registro}")
                return False

#  FILA DE TRANSAÇÕES (Padrão Produtor-Consumidor)
def agencia_online(conta, fila_transacoes, nome_agencia):
    """Thread PRODUTORA: Apenas cria os pedidos e joga na fila, não mexe no saldo diretamente."""
    operacoes = [
        ("deposito", 500.00),
        ("saque",    200.00),
        ("deposito", 1200.00),
        ("saque",    800.00),
        ("deposito", 300.00),
        ("saque",    1500.00),
    ]
    for tipo, valor in operacoes:
        # Monta o pedido como um dicionário
        transacao = {"tipo": tipo, "valor": valor, "agencia": nome_agencia}
        # .put() coloca o pedido no final da fila com segurança
        fila_transacoes.put(transacao)
        print(f"   [{nome_agencia}] Enfileirou: {tipo.upper()} R${valor:.2f}")
        # Simula atrasos de rede diferentes para cada transação chegar
        time.sleep(random.uniform(0.05, 0.15))

    # Quando a agência termina sua lista, envia um 'None' como sinalização de encerramento
    fila_transacoes.put(None)  


def processador_central(conta, fila_transacoes, total_agencias):
    """Thread CONSUMIDORA: Fica lendo a fila e executando os pedidos um por vez."""
    finalizadas = 0
    # Continua rodando até que todas as agências enviem o sinal de encerramento ('None')
    while finalizadas < total_agencias:
        # .get() pega o próximo pedido da fila. Se a fila estiver vazia, a thread espera.
        transacao = fila_transacoes.get()
        
        # Se recebeu o sinal de encerramento, anota que uma agência terminou e volta pro loop
        if transacao is None:
            finalizadas += 1
            continue

        # Executa de fato a operação chamando os métodos protegidos por Lock na Conta
        if transacao["tipo"] == "deposito":
            conta.depositar(transacao["valor"], transacao["agencia"])
        elif transacao["tipo"] == "saque":
            conta.sacar(transacao["valor"], transacao["agencia"])

        # Sinaliza para o objeto Fila que esta transação específica foi totalmente concluída
        fila_transacoes.task_done()


#  CENÁRIO SEM LOCK (Demonstração da Falha)
def transferencia_sem_protecao(saldo_ref, valor, nome):
    """ PERIGOSO: race condition porque não há cadeado (Lock) bloqueando acessos simultâneos."""
    lido = saldo_ref[0]
    # Pausamos a thread aqui por 1 milissegundo. 
    # Isso dá tempo suficiente para outra thread ler o saldo antigo antes que a atual atualize o valor.
    time.sleep(0.001)          
    saldo_ref[0] = lido - valor
    print(f"    [{nome}] Retirou R${valor:.2f} (sem lock) → saldo: R${saldo_ref[0]:.2f}")


#  EXECUÇÃO PRINCIPAL
def demonstrar_race_condition():
    print("\n" + "="*60)
    print("    DEMONSTRAÇÃO: RACE CONDITION (sem Lock)")
    print("="*60)
    # Usamos uma lista para o saldo inicial
    saldo = [1000.0]   
    threads = []
    
    # Cria 5 threads que tentarão sacar juntas
    for i in range(5):
        t = threading.Thread(target=transferencia_sem_protecao,
                             args=(saldo, 100.0, f"Thread-{i+1}"))
        threads.append(t)
        
    # .start() dá a ordem para todas as 5 threads começarem a correr ao mesmo tempo
    for t in threads:
        t.start()
        
    # .join() faz o programa principal "sentar e esperar" as 5 threads terminarem antes de imprimir o saldo final
    for t in threads:
        t.join()
        
    print(f"\n  Saldo final (esperado R$500.00): R${saldo[0]:.2f}")
    print("  → Com race condition o resultado pode ser INCORRETO!\n")


def main():
    print("="*60)
    print("    BANCO PYTHON — SISTEMA CONCORRENTE")
    print("="*60)

    # ── 1. Roda a prova de que código sem Lock falha ──
    demonstrar_race_condition()

    # ── 2. Inicia o cenário organizado e seguro ──
    print("="*60)
    print("  SOLUÇÃO: Lock + Queue (Memória Compartilhada Segura)")
    print("="*60)

    # Instancia os objetos principais
    conta = ContaBancaria("João Silva", saldo_inicial=1000.00)
    fila = Queue()

    print(f"\n   Conta: {conta.titular} | Saldo inicial: R${conta.saldo:.2f}\n")
    print("   Iniciando agências e processador central...\n")

    # Mapeia as Threads apontando para a função agencia_online
    agencia1 = threading.Thread(target=agencia_online,
                                args=(conta, fila, "Agência Rio"))
    agencia2 = threading.Thread(target=agencia_online,
                                args=(conta, fila, "App Mobile"))

    # Mapeia a Thread CONSUMIDORA. O '2' indica que ela deve esperar o encerramento de 2 agências.
    processador = threading.Thread(target=processador_central,
                                   args=(conta, fila, 2))

    # Dá o play em todo mundo
    processador.start()
    agencia1.start()
    agencia2.start()

    # Bloqueia a execução do main() até que agências e processador terminem seus trabalhos
    agencia1.join()
    agencia2.join()
    processador.join()

    # Como usamos Lock e Queue, a matemática chegará intacta aqui
    print("\n" + "="*60)
    print(f"   RESULTADO FINAL")
    print("="*60)
    print(f"  Titular : {conta.titular}")
    print(f"  Saldo   : R${conta.saldo:.2f}")
    print(f"  Total de operações: {len(conta.historico)}")
    print("="*60)

if __name__ == "__main__":
    main()
